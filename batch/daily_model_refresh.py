#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from google.cloud import bigquery
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


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
    parser = argparse.ArgumentParser(description="Daily Spark model refresh job")
    parser.add_argument(
        "--training-source",
        choices=["bigquery", "csv"],
        default="bigquery",
        help="Source for model training dataset",
    )
    parser.add_argument(
        "--silver-path",
        default="data/lake/silver/scored_transactions",
        help="Input Silver scored transactions path (parquet), used when training-source=csv",
    )
    parser.add_argument(
        "--labels-csv",
        default="data/transaction_log.csv",
        help="Historical labeled CSV path containing isFraud, used when training-source=csv",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("FRAUD_GCP_PROJECT_ID", ""),
        help="BigQuery project id, used when training-source=bigquery",
    )
    parser.add_argument(
        "--dataset",
        default=os.environ.get("FRAUD_BQ_DATASET", "fraud_analytics"),
        help="BigQuery dataset containing hourly batch outputs",
    )
    parser.add_argument(
        "--retraining-table",
        default="retraining_dataset",
        help="BigQuery table containing retraining rows",
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


def is_cloud_uri(path: str) -> bool:
    return path.startswith("gs://")


def validate_local_paths(args: argparse.Namespace) -> None:
    if args.training_source == "csv":
        if not is_cloud_uri(args.silver_path):
            silver_path = Path(args.silver_path)
            if not silver_path.exists():
                raise FileNotFoundError(f"Silver path not found: {silver_path}")

        if not is_cloud_uri(args.labels_csv):
            labels_csv = Path(args.labels_csv)
            if not labels_csv.exists():
                raise FileNotFoundError(f"Labels CSV not found: {labels_csv}")

    if args.training_source == "bigquery" and not args.project_id.strip():
        raise ValueError("project-id is required when training-source=bigquery")

    if not is_cloud_uri(args.model_output):
        model_output = Path(args.model_output)
        model_output.parent.mkdir(parents=True, exist_ok=True)


def ensure_gcs_credentials(paths: list[str]) -> str | None:
    uses_gcs = any(is_cloud_uri(path) for path in paths)
    gcs_credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if uses_gcs and not gcs_credentials_file:
        raise ValueError(
            "GCS paths detected but GOOGLE_APPLICATION_CREDENTIALS is not set. "
            "Set it to your service account key JSON file path."
        )
    return gcs_credentials_file or None


def build_spark(gcs_enabled: bool, gcs_credentials_file: str | None) -> SparkSession:
    builder = SparkSession.builder.appName("fraud-daily-model-refresh").master("local[*]")

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


def deduplicate_scored(scored_df: DataFrame) -> DataFrame:
    if {"source_topic", "source_partition", "source_offset"}.issubset(set(scored_df.columns)):
        dedupe_keys = ["source_topic", "source_partition", "source_offset"]
    else:
        dedupe_keys = JOIN_KEYS + ["event_ts"]
    return scored_df.dropDuplicates(dedupe_keys)


def build_model_refresh_dataset_from_csv(scored_df: DataFrame, labels_df: DataFrame) -> DataFrame:
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
    return historical_scored_df.join(labels_only_df, on=JOIN_KEYS, how="inner")


def build_model_refresh_dataset_from_bigquery(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    table_ref = f"`{args.project_id}.{args.dataset}.{args.retraining_table}`"
    query = f"""
select
  cast(step as int64) as step,
  cast(type as string) as type,
  cast(amount as float64) as amount,
  cast(nameOrig as string) as nameOrig,
  cast(nameDest as string) as nameDest,
  cast(oldbalanceOrg as float64) as oldbalanceOrg,
  cast(newbalanceOrig as float64) as newbalanceOrig,
  cast(oldbalanceDest as float64) as oldbalanceDest,
  cast(newbalanceDest as float64) as newbalanceDest,
  cast(isFraud as float64) as isFraud,
  cast(velocity_5min as float64) as velocity_5min,
  cast(balance_change_ratio as float64) as balance_change_ratio,
  cast(is_new_merchant as float64) as is_new_merchant,
  cast(origin_balance_delta as float64) as origin_balance_delta,
  cast(dest_balance_delta as float64) as dest_balance_delta
from {table_ref}
where isFraud is not null
"""

    bq_client = bigquery.Client(project=args.project_id)
    pandas_df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    if pandas_df.empty:
        return spark.createDataFrame([], schema="step int, type string, amount double, nameOrig string, nameDest string, oldbalanceOrg double, newbalanceOrig double, oldbalanceDest double, newbalanceDest double, isFraud double, velocity_5min double, balance_change_ratio double, is_new_merchant double, origin_balance_delta double, dest_balance_delta double")
    return spark.createDataFrame(pandas_df)


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


def main() -> None:
    args = parse_args()
    gcs_paths = [args.model_output]
    if args.training_source == "csv":
        gcs_paths.extend([args.silver_path, args.labels_csv])

    gcs_credentials_file = ensure_gcs_credentials(gcs_paths)
    validate_local_paths(args)

    spark = build_spark(
        gcs_enabled=any(is_cloud_uri(path) for path in gcs_paths),
        gcs_credentials_file=gcs_credentials_file,
    )

    if args.training_source == "bigquery":
        model_training_df = build_model_refresh_dataset_from_bigquery(spark, args)
        training_input_desc = f"bigquery:{args.project_id}.{args.dataset}.{args.retraining_table}"
    else:
        scored_df = spark.read.parquet(args.silver_path)
        scored_df = scored_df.withColumn("event_ts", F.to_timestamp(F.col("event_ts")))
        scored_df = deduplicate_scored(scored_df)

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
        model_training_df = build_model_refresh_dataset_from_csv(scored_df, labels_df)
        training_input_desc = f"csv:{args.labels_csv} + silver:{args.silver_path}"

    refreshed, refresh_message = train_and_save_refreshed_model(model_training_df, args)

    print("Daily model refresh completed.")
    print(f"Training input: {training_input_desc}")
    print(f"Model output: {args.model_output}")
    print(refresh_message)
    if not refreshed:
        print("Serving model artifact left unchanged.")

    spark.stop()


if __name__ == "__main__":
    main()
