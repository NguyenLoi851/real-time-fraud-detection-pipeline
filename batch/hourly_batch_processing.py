#!/usr/bin/env python3
"""
Goal: Process hourly Spark batch data for reconciliation and data warehouse.
Joins scored transactions with historical labels, computes monitoring metrics,
and outputs curated datasets for model retraining and performance monitoring.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


JOIN_KEYS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "nameDest",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hourly Spark batch for reconciliation and warehouse-ready datasets")
    parser.add_argument(
        "--silver-path",
        default="data/lake/silver/scored_transactions",
        help="Input Silver scored transactions path (parquet)",
    )
    parser.add_argument(
        "--labels-csv",
        default="data/transaction_log.csv",
        help="Historical labeled CSV path containing isFraud",
    )
    parser.add_argument(
        "--output-base",
        default="data/lake/gold/hourly_batch",
        help="Output base path for curated and monitoring datasets",
    )
    parser.add_argument(
        "--target-hour-utc",
        default=None,
        help="Optional target hour in UTC format YYYY-MM-DD-HH; if omitted, process all data",
    )
    return parser.parse_args()


def validate_local_paths(args: argparse.Namespace) -> None:
    if not is_cloud_uri(args.silver_path):
        silver_path = Path(args.silver_path)
        if not silver_path.exists():
            raise FileNotFoundError(f"Silver path not found: {silver_path}")

    if not is_cloud_uri(args.labels_csv):
        labels_csv = Path(args.labels_csv)
        if not labels_csv.exists():
            raise FileNotFoundError(f"Labels CSV not found: {labels_csv}")

    if not is_cloud_uri(args.output_base):
        output_base = Path(args.output_base)
        output_base.mkdir(parents=True, exist_ok=True)



def build_spark(gcs_enabled: bool, gcs_credentials_file: str | None) -> SparkSession:
    builder = (
        SparkSession.builder.appName("fraud-hourly-batch")
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if gcs_enabled:
        builder = (
            builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        )
        if gcs_credentials_file:
            builder = (
                builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_credentials_file)
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def is_cloud_uri(path: str) -> bool:
    return path.startswith("gs://")


def join_output_path(base_path: str, child: str) -> str:
    if is_cloud_uri(base_path):
        return f"{base_path.rstrip('/')}/{child}"
    return str(Path(base_path) / child)


def ensure_gcs_credentials(paths: list[str]) -> str | None:
    uses_gcs = any(is_cloud_uri(path) for path in paths)
    gcs_credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if uses_gcs and not gcs_credentials_file:
        raise ValueError(
            "GCS paths detected but GOOGLE_APPLICATION_CREDENTIALS is not set. "
            "Set it to your service account key JSON file path."
        )
    return gcs_credentials_file or None


def deduplicate_scored(scored_df: DataFrame) -> DataFrame:
    if {"source_topic", "source_partition", "source_offset"}.issubset(set(scored_df.columns)):
        dedupe_keys = ["source_topic", "source_partition", "source_offset"]
    else:
        dedupe_keys = JOIN_KEYS + ["event_ts"]
    return scored_df.dropDuplicates(dedupe_keys)


def reconcile_labels(scored_df: DataFrame, labels_df: DataFrame) -> DataFrame:
    reconciled_df = scored_df.join(labels_df, on=JOIN_KEYS, how="left")

    return (
        reconciled_df.withColumn("isFraud", F.col("isFraud").cast("double"))
        .withColumn("is_label_available", F.col("isFraud").isNotNull())
        .withColumn(
            "label_delay_hours",
            F.when(
                F.col("isFraud").isNotNull(),
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("event_ts"))) / F.lit(3600.0),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def build_training_dataset(reconciled_df: DataFrame) -> DataFrame:
    labeled_df = reconciled_df.filter(F.col("is_label_available") == F.lit(True))

    counts = labeled_df.groupBy("isFraud").count()
    count_map = {int(row["isFraud"]): int(row["count"]) for row in counts.collect()}

    fraud_count = max(count_map.get(1, 1), 1)
    nonfraud_count = max(count_map.get(0, 1), 1)
    total_count = fraud_count + nonfraud_count

    fraud_weight = total_count / (2.0 * fraud_count)
    nonfraud_weight = total_count / (2.0 * nonfraud_count)

    with_weights_df = labeled_df.withColumn(
        "class_weight",
        F.when(F.col("isFraud") == F.lit(1.0), F.lit(fraud_weight)).otherwise(F.lit(nonfraud_weight)),
    )

    split_window = Window.orderBy(F.col("event_ts").asc_nulls_last())
    split_df = with_weights_df.withColumn("split_rank", F.percent_rank().over(split_window))

    return (
        split_df.withColumn(
            "dataset_split",
            F.when(F.col("split_rank") < F.lit(0.7), F.lit("train"))
            .when(F.col("split_rank") < F.lit(0.85), F.lit("validation"))
            .otherwise(F.lit("test")),
        ).drop("split_rank")
    )


def build_hourly_monitoring(reconciled_df: DataFrame) -> DataFrame:
    return (
        reconciled_df.withColumn("event_hour_utc", F.date_trunc("hour", F.col("event_ts")))
        .groupBy("event_hour_utc", "type")
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum(F.when(F.col("is_alert") == F.lit(True), F.lit(1)).otherwise(F.lit(0))).alias("alert_count"),
            F.avg(F.col("fraud_score")).alias("avg_fraud_score"),
            F.expr("percentile_approx(fraud_score, 0.95)").alias("p95_fraud_score"),
            F.avg(F.when(F.col("is_label_available") == F.lit(True), F.col("isFraud"))).alias("observed_fraud_rate"),
        )
        .withColumn("alert_rate", F.when(F.col("txn_count") > 0, F.col("alert_count") / F.col("txn_count")).otherwise(F.lit(0.0)))
    )


def maybe_filter_hour(df: DataFrame, target_hour_utc: str | None) -> DataFrame:
    if not target_hour_utc:
        return df
    return df.filter(F.date_format(F.col("event_ts"), "yyyy-MM-dd-HH") == F.lit(target_hour_utc))


def main() -> None:
    args = parse_args()
    gcs_credentials_file = ensure_gcs_credentials([args.silver_path, args.labels_csv, args.output_base])
    validate_local_paths(args)

    spark = build_spark(
        gcs_enabled=any(is_cloud_uri(path) for path in [args.silver_path, args.labels_csv, args.output_base]),
        gcs_credentials_file=gcs_credentials_file,
    )

    scored_df = spark.read.parquet(args.silver_path)
    scored_df = scored_df.withColumn("event_ts", F.to_timestamp(F.col("event_ts")))
    scored_df = deduplicate_scored(scored_df)
    scored_df = maybe_filter_hour(scored_df, args.target_hour_utc)

    labels_df = (
        spark.read.option("header", True)
        .csv(args.labels_csv)
        .select(
            F.col("step").cast("int").alias("step"),
            F.col("type").alias("type"),
            F.col("amount").cast("double").alias("amount"),
            F.col("nameOrig").alias("nameOrig"),
            F.col("nameDest").alias("nameDest"),
            F.col("oldbalanceOrg").cast("double").alias("oldbalanceOrg"),
            F.col("newbalanceOrig").cast("double").alias("newbalanceOrig"),
            F.col("oldbalanceDest").cast("double").alias("oldbalanceDest"),
            F.col("newbalanceDest").cast("double").alias("newbalanceDest"),
            F.col("isFraud").cast("double").alias("isFraud"),
        )
        .dropDuplicates(JOIN_KEYS)
    )

    reconciled_df = reconcile_labels(scored_df, labels_df)
    training_df = build_training_dataset(reconciled_df)
    monitoring_df = build_hourly_monitoring(reconciled_df)

    reconciled_output_df = reconciled_df.withColumn("event_hour_utc", F.date_trunc("hour", F.col("event_ts")))
    training_output_df = training_df.withColumn("event_hour_utc", F.date_trunc("hour", F.col("event_ts")))

    curated_path = join_output_path(args.output_base, "curated_scored")
    training_path = join_output_path(args.output_base, "retraining_dataset")
    monitoring_path = join_output_path(args.output_base, "monitoring_hourly")

    reconciled_output_df.write.mode("overwrite").partitionBy("event_hour_utc").parquet(curated_path)
    training_output_df.write.mode("overwrite").partitionBy("event_hour_utc").parquet(training_path)
    monitoring_df.write.mode("overwrite").partitionBy("event_hour_utc").parquet(monitoring_path)

    print("Batch processing completed.")
    print(f"Input Silver path: {args.silver_path}")
    print(f"Input labels CSV: {args.labels_csv}")
    print(f"Curated scored output: {curated_path}")
    print(f"Retraining dataset output: {training_path}")
    print(f"Monitoring output: {monitoring_path}")

    spark.stop()


if __name__ == "__main__":
    main()
