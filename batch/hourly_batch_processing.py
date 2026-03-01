#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
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
    parser = argparse.ArgumentParser(description="Hourly Spark batch for reconciliation and model refresh datasets")
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
    parser.add_argument(
        "--model-output",
        default="ml/artifacts/fraud_rf_pipeline",
        help="Output path for refreshed Spark PipelineModel",
    )
    parser.add_argument(
        "--model-seed",
        type=int,
        default=42,
        help="Random seed for refreshed model training",
    )
    parser.add_argument(
        "--model-num-trees",
        type=int,
        default=120,
        help="RandomForest numTrees for refreshed model",
    )
    parser.add_argument(
        "--model-max-depth",
        type=int,
        default=8,
        help="RandomForest maxDepth for refreshed model",
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

    if not is_cloud_uri(args.model_output):
        model_output = Path(args.model_output)
        model_output.parent.mkdir(parents=True, exist_ok=True)


def build_spark(gcs_enabled: bool, gcs_credentials_file: str | None) -> SparkSession:
    builder = SparkSession.builder.appName("fraud-hourly-batch").master("local[*]")

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


def prepare_model_features(training_df: DataFrame) -> DataFrame:
    velocity_expr = F.col("velocity_5min").cast("double") if "velocity_5min" in training_df.columns else F.lit(0.0)
    ratio_expr = (
        F.col("balance_change_ratio").cast("double")
        if "balance_change_ratio" in training_df.columns
        else F.when(F.col("oldbalanceOrg") > 0, F.col("amount") / F.col("oldbalanceOrg")).otherwise(F.lit(0.0))
    )
    new_merchant_expr = (
        F.col("is_new_merchant").cast("double")
        if "is_new_merchant" in training_df.columns
        else F.when((F.col("oldbalanceDest") == 0.0) & (F.col("newbalanceDest") == 0.0), F.lit(1.0)).otherwise(F.lit(0.0))
    )
    origin_delta_expr = (
        F.col("origin_balance_delta").cast("double")
        if "origin_balance_delta" in training_df.columns
        else F.col("oldbalanceOrg") - F.col("newbalanceOrig")
    )
    dest_delta_expr = (
        F.col("dest_balance_delta").cast("double")
        if "dest_balance_delta" in training_df.columns
        else F.col("newbalanceDest") - F.col("oldbalanceDest")
    )

    return (
        training_df.withColumn("step", F.coalesce(F.col("step").cast("int"), F.lit(0)))
        .withColumn("amount", F.coalesce(F.col("amount").cast("double"), F.lit(0.0)))
        .withColumn("oldbalanceOrg", F.coalesce(F.col("oldbalanceOrg").cast("double"), F.lit(0.0)))
        .withColumn("newbalanceOrig", F.coalesce(F.col("newbalanceOrig").cast("double"), F.lit(0.0)))
        .withColumn("oldbalanceDest", F.coalesce(F.col("oldbalanceDest").cast("double"), F.lit(0.0)))
        .withColumn("newbalanceDest", F.coalesce(F.col("newbalanceDest").cast("double"), F.lit(0.0)))
        .withColumn("velocity_5min", F.coalesce(velocity_expr, F.lit(0.0)))
        .withColumn("balance_change_ratio", F.coalesce(ratio_expr, F.lit(0.0)))
        .withColumn("is_new_merchant", F.coalesce(new_merchant_expr, F.lit(0.0)))
        .withColumn("origin_balance_delta", F.coalesce(origin_delta_expr, F.lit(0.0)))
        .withColumn("dest_balance_delta", F.coalesce(dest_delta_expr, F.lit(0.0)))
        .withColumn("type", F.coalesce(F.col("type"), F.lit("UNKNOWN")))
        .withColumn("isFraud", F.col("isFraud").cast("double"))
    )


def newest_hour_slice(scored_df: DataFrame, target_hour_utc: str | None) -> DataFrame:
    if target_hour_utc:
        return scored_df.filter(F.date_format(F.col("event_ts"), "yyyy-MM-dd-HH") == F.lit(target_hour_utc))

    max_hour_row = (
        scored_df.withColumn("event_hour_utc", F.date_trunc("hour", F.col("event_ts")))
        .agg(F.max("event_hour_utc").alias("max_hour"))
        .collect()[0]
    )
    max_hour = max_hour_row["max_hour"]
    if max_hour is None:
        return scored_df.limit(0)

    return scored_df.filter(F.date_trunc("hour", F.col("event_ts")) == F.lit(max_hour))


def build_model_refresh_dataset(scored_df: DataFrame, labels_df: DataFrame, target_hour_utc: str | None) -> DataFrame:
    scored_feature_cols = [
        "velocity_5min",
        "balance_change_ratio",
        "is_new_merchant",
        "origin_balance_delta",
        "dest_balance_delta",
    ]
    available_feature_cols = [column for column in scored_feature_cols if column in scored_df.columns]

    labels_only_df = labels_df.select(*JOIN_KEYS, F.col("isFraud").cast("double").alias("isFraud"))

    historical_scored_df = scored_df.select(*JOIN_KEYS, *available_feature_cols).dropDuplicates(JOIN_KEYS)
    historical_labeled_df = historical_scored_df.join(labels_only_df, on=JOIN_KEYS, how="inner")

    newest_df = newest_hour_slice(scored_df, target_hour_utc).select(*JOIN_KEYS, *available_feature_cols)
    newest_labeled_df = newest_df.join(labels_only_df, on=JOIN_KEYS, how="inner")

    return historical_labeled_df.unionByName(newest_labeled_df, allowMissingColumns=True).dropDuplicates(JOIN_KEYS)


def train_and_save_refreshed_model(training_df: DataFrame, args: argparse.Namespace) -> tuple[bool, str]:
    labeled_df = training_df.filter(F.col("isFraud").isNotNull())
    labeled_count = labeled_df.count()
    if labeled_count == 0:
        return False, "No labeled records available in refresh dataset; skipped model refresh."

    label_values = {int(row["isFraud"]) for row in labeled_df.select("isFraud").distinct().collect()}
    if label_values != {0, 1}:
        return False, (
            f"Need both fraud/non-fraud classes for training, found labels={sorted(label_values)}; "
            "skipped model refresh."
        )

    prepared_df = prepare_model_features(labeled_df)

    counts = prepared_df.groupBy("isFraud").count()
    count_map = {int(row["isFraud"]): int(row["count"]) for row in counts.collect()}
    fraud_count = max(count_map.get(1, 1), 1)
    nonfraud_count = max(count_map.get(0, 1), 1)
    total_count = fraud_count + nonfraud_count

    fraud_weight = total_count / (2.0 * fraud_count)
    nonfraud_weight = total_count / (2.0 * nonfraud_count)

    weighted_df = prepared_df.withColumn(
        "class_weight",
        F.when(F.col("isFraud") == F.lit(1.0), F.lit(fraud_weight)).otherwise(F.lit(nonfraud_weight)),
    )

    train_df, test_df = weighted_df.randomSplit([0.8, 0.2], seed=args.model_seed)
    if train_df.rdd.isEmpty() or test_df.rdd.isEmpty():
        return False, "Insufficient data after split for train/test; skipped model refresh."

    type_indexer = StringIndexer(inputCol="type", outputCol="type_index", handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=[
            "step",
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "velocity_5min",
            "balance_change_ratio",
            "is_new_merchant",
            "origin_balance_delta",
            "dest_balance_delta",
            "type_index",
        ],
        outputCol="features",
        handleInvalid="keep",
    )
    classifier = RandomForestClassifier(
        labelCol="isFraud",
        featuresCol="features",
        predictionCol="prediction",
        probabilityCol="probability",
        weightCol="class_weight",
        numTrees=args.model_num_trees,
        maxDepth=args.model_max_depth,
        seed=args.model_seed,
    )

    pipeline = Pipeline(stages=[type_indexer, assembler, classifier])
    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(labelCol="isFraud", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    model.write().overwrite().save(args.model_output)
    return True, f"Refreshed model saved to {args.model_output} with test AUC={auc:.4f}"


def maybe_filter_hour(df: DataFrame, target_hour_utc: str | None) -> DataFrame:
    if not target_hour_utc:
        return df
    return df.filter(F.date_format(F.col("event_ts"), "yyyy-MM-dd-HH") == F.lit(target_hour_utc))


def main() -> None:
    args = parse_args()
    gcs_credentials_file = ensure_gcs_credentials([args.silver_path, args.labels_csv, args.output_base, args.model_output])
    validate_local_paths(args)

    spark = build_spark(
        gcs_enabled=any(is_cloud_uri(path) for path in [args.silver_path, args.labels_csv, args.output_base, args.model_output]),
        gcs_credentials_file=gcs_credentials_file,
    )

    full_scored_df = spark.read.parquet(args.silver_path)
    full_scored_df = full_scored_df.withColumn("event_ts", F.to_timestamp(F.col("event_ts")))
    full_scored_df = deduplicate_scored(full_scored_df)

    scored_df = maybe_filter_hour(full_scored_df, args.target_hour_utc)

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

    curated_path = join_output_path(args.output_base, "curated_scored")
    training_path = join_output_path(args.output_base, "retraining_dataset")
    monitoring_path = join_output_path(args.output_base, "monitoring_hourly")

    reconciled_df.write.mode("overwrite").parquet(curated_path)
    training_df.write.mode("overwrite").parquet(training_path)
    monitoring_df.write.mode("overwrite").parquet(monitoring_path)

    model_training_df = build_model_refresh_dataset(full_scored_df, labels_df, args.target_hour_utc)
    refreshed, refresh_message = train_and_save_refreshed_model(model_training_df, args)

    print("Batch processing completed.")
    print(f"Input Silver path: {args.silver_path}")
    print(f"Input labels CSV: {args.labels_csv}")
    print(f"Curated scored output: {curated_path}")
    print(f"Retraining dataset output: {training_path}")
    print(f"Monitoring output: {monitoring_path}")
    print(refresh_message)
    if not refreshed:
        print("Serving model artifact left unchanged.")

    spark.stop()


if __name__ == "__main__":
    main()
