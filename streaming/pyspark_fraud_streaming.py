#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local PySpark Kafka fraud scoring stream")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--input-topic", default="transactions_raw", help="Kafka input topic")
    parser.add_argument("--alerts-topic", default="fraud_alerts", help="Kafka topic for fraud alerts")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"], help="Kafka offsets mode")
    parser.add_argument("--model-path", default="ml/artifacts/fraud_rf_pipeline", help="Path to Spark pre-trained PipelineModel")
    parser.add_argument("--checkpoint-dir", default="data/checkpoints/fraud_stream", help="Checkpoint directory")
    parser.add_argument("--datalake-raw-path", default="data/lake/bronze/transactions_raw", help="Raw records data lake path")
    parser.add_argument("--datalake-scored-path", default="data/lake/silver/scored_transactions", help="Scored records data lake path")
    parser.add_argument("--datalake-alerts-path", default="data/lake/gold/fraud_alerts", help="Alert records data lake path")
    parser.add_argument("--fraud-score-threshold", type=float, default=0.8, help="Threshold for high fraud score")
    parser.add_argument("--high-amount-threshold", type=float, default=200000.0, help="Amount threshold for transfer/cash-out alert rule")
    parser.add_argument("--velocity-threshold", type=int, default=5, help="Minimum velocity_5min to trigger alert rule")
    parser.add_argument("--trigger-seconds", type=int, default=10, help="Micro-batch trigger interval")
    return parser.parse_args()


def build_schema() -> StructType:
    return StructType(
        [
            StructField("step", StringType(), True),
            StructField("type", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("nameOrig", StringType(), True),
            StructField("oldbalanceOrg", StringType(), True),
            StructField("newbalanceOrig", StringType(), True),
            StructField("nameDest", StringType(), True),
            StructField("oldbalanceDest", StringType(), True),
            StructField("newbalanceDest", StringType(), True),
            StructField("event_emitted_at_utc", StringType(), True),
        ]
    )


def engineer_features(df: DataFrame) -> DataFrame:
    typed_df = (
        df.withColumn("step", F.coalesce(F.col("step").cast(IntegerType()), F.lit(0)))
        .withColumn("type", F.coalesce(F.col("type"), F.lit("UNKNOWN")))
        .withColumn("amount", F.coalesce(F.col("amount").cast(DoubleType()), F.lit(0.0)))
        .withColumn("nameOrig", F.coalesce(F.col("nameOrig"), F.lit("UNKNOWN")))
        .withColumn("oldbalanceOrg", F.coalesce(F.col("oldbalanceOrg").cast(DoubleType()), F.lit(0.0)))
        .withColumn("newbalanceOrig", F.coalesce(F.col("newbalanceOrig").cast(DoubleType()), F.lit(0.0)))
        .withColumn("nameDest", F.coalesce(F.col("nameDest"), F.lit("UNKNOWN")))
        .withColumn("oldbalanceDest", F.coalesce(F.col("oldbalanceDest").cast(DoubleType()), F.lit(0.0)))
        .withColumn("newbalanceDest", F.coalesce(F.col("newbalanceDest").cast(DoubleType()), F.lit(0.0)))
        .withColumn("event_ts", F.coalesce(F.to_timestamp("event_emitted_at_utc"), F.current_timestamp()))
    )

    with_time_bucket = typed_df.withColumn("time_bucket_5min", F.floor(F.unix_timestamp("event_ts") / F.lit(300)).cast("long"))

    velocity_window = Window.partitionBy("nameOrig", "time_bucket_5min")
    featured_df = (
        with_time_bucket.withColumn("velocity_5min", F.count(F.lit(1)).over(velocity_window).cast(DoubleType()))
        .withColumn("balance_change_ratio", F.when(F.col("oldbalanceOrg") > 0, F.col("amount") / F.col("oldbalanceOrg")).otherwise(F.lit(0.0)))
        .withColumn("is_new_merchant", F.when((F.col("oldbalanceDest") == 0.0) & (F.col("newbalanceDest") == 0.0), F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("origin_balance_delta", F.col("oldbalanceOrg") - F.col("newbalanceOrig"))
        .withColumn("dest_balance_delta", F.col("newbalanceDest") - F.col("oldbalanceDest"))
    )
    return featured_df


def build_spark(app_name: str, gcs_enabled: bool, gcs_credentials_file: str | None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master("local[*]")

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


def main() -> None:
    args = parse_args()

    uses_gcs = any(
        is_cloud_uri(path)
        for path in [args.model_path, args.checkpoint_dir, args.datalake_raw_path, args.datalake_scored_path, args.datalake_alerts_path]
    )
    gcs_credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    if uses_gcs and not gcs_credentials_file:
        raise ValueError(
            "GCS paths detected but GOOGLE_APPLICATION_CREDENTIALS is not set. "
            "Set it to your service account key JSON file path."
        )

    if not is_cloud_uri(args.model_path):
        if not Path(args.model_path).exists():
            raise FileNotFoundError(
                f"Pre-trained model not found: {args.model_path}. Train first with: spark-submit ml/train_fraud_model.py"
            )

    for target_path in [args.checkpoint_dir, args.datalake_raw_path, args.datalake_scored_path, args.datalake_alerts_path]:
        if not is_cloud_uri(target_path):
            Path(target_path).mkdir(parents=True, exist_ok=True)

    spark = build_spark(
        app_name="fraud-streaming-local-pyspark",
        gcs_enabled=uses_gcs,
        gcs_credentials_file=gcs_credentials_file or None,
    )
    model = PipelineModel.load(args.model_path)

    kafka_options = {
        "kafka.bootstrap.servers": args.bootstrap_servers,
    }

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    json_schema = build_schema()

    parsed_stream = (
        raw_stream.select(
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
            F.col("timestamp").alias("kafka_ingest_ts"),
            F.from_json(F.col("value").cast("string"), json_schema).alias("event"),
        )
        .select("source_topic", "source_partition", "source_offset", "kafka_ingest_ts", "event.*")
    )

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            print(f"Batch {batch_id}: no records in this micro-batch", flush=True)
            return

        raw_df = batch_df.withColumn(
            "event_ts",
            F.coalesce(F.to_timestamp("event_emitted_at_utc"), F.current_timestamp()),
        )

        raw_write_cols = [
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_ingest_ts",
            "event_emitted_at_utc",
            "event_ts",
            "step",
            "type",
            "amount",
            "nameOrig",
            "oldbalanceOrg",
            "newbalanceOrig",
            "nameDest",
            "oldbalanceDest",
            "newbalanceDest",
        ]

        raw_df.select(*raw_write_cols).write.mode("append").parquet(args.datalake_raw_path)

        raw_count = raw_df.count()
        print(
            f"Batch {batch_id}: raw write success path={args.datalake_raw_path} records={raw_count}",
            flush=True,
        )

        featured_df = engineer_features(batch_df)
        scored_df = model.transform(featured_df)

        scored_df = (
            scored_df.withColumn("fraud_score", F.coalesce(vector_to_array("probability")[1], F.lit(0.0)))
            .withColumn("predicted_is_fraud", (F.col("prediction") == 1.0))
        )

        high_score_rule = F.col("fraud_score") >= F.lit(args.fraud_score_threshold)
        high_amount_rule = (
            F.col("type").isin("TRANSFER", "CASH_OUT")
            & (F.col("amount") >= F.lit(args.high_amount_threshold))
        )
        high_velocity_rule = F.col("velocity_5min") >= F.lit(float(args.velocity_threshold))

        final_df = (
            scored_df.withColumn("rule_high_score", high_score_rule)
            .withColumn("rule_high_amount_transfer", high_amount_rule)
            .withColumn("rule_high_velocity", high_velocity_rule)
            .withColumn("is_alert", high_score_rule | high_amount_rule | high_velocity_rule)
        )

        write_cols = [
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_ingest_ts",
            "event_emitted_at_utc",
            "event_ts",
            "step",
            "type",
            "amount",
            "nameOrig",
            "nameDest",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "velocity_5min",
            "balance_change_ratio",
            "is_new_merchant",
            "origin_balance_delta",
            "dest_balance_delta",
            "fraud_score",
            "predicted_is_fraud",
            "is_alert",
            "rule_high_score",
            "rule_high_amount_transfer",
            "rule_high_velocity",
        ]

        final_df.select(*write_cols).write.mode("append").parquet(args.datalake_scored_path)

        scored_count = final_df.count()
        print(
            f"Batch {batch_id}: scored write success path={args.datalake_scored_path} records={scored_count}",
            flush=True,
        )

        alerts_df = final_df.filter(F.col("is_alert"))
        alerts_df.select(*write_cols).write.mode("append").parquet(args.datalake_alerts_path)

        alert_count = alerts_df.count()
        print(
            f"Batch {batch_id}: alerts write success path={args.datalake_alerts_path} records={alert_count}",
            flush=True,
        )

        alerts_kafka_df = alerts_df.select(
            F.col("nameOrig").cast("string").alias("key"),
            F.to_json(F.struct(*[F.col(c) for c in write_cols])).alias("value"),
        )
        alerts_kafka_df.write.format("kafka").options(**kafka_options).option("topic", args.alerts_topic).save()
        print(
            f"Batch {batch_id}: kafka publish success topic={args.alerts_topic} records={alert_count}",
            flush=True,
        )

        alert_summary = alerts_df.agg(
            F.count(F.lit(1)).alias("alert_count"),
            F.max("fraud_score").alias("max_fraud_score"),
        ).collect()[0]

        alert_count_summary = int(alert_summary["alert_count"]) if alert_summary["alert_count"] is not None else 0
        max_score = float(alert_summary["max_fraud_score"]) if alert_summary["max_fraud_score"] is not None else 0.0

        print(
            f"Batch {batch_id}: processed={scored_count} alerts={alert_count_summary} max_fraud_score={max_score:.4f}",
            flush=True,
        )

    query = (
        parsed_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    print("Fraud streaming job started. Press Ctrl+C to stop.", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
