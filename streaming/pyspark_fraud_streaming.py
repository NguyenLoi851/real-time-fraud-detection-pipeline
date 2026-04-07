#!/usr/bin/env python3
"""Real-time fraud scoring stream (Pub/Sub -> PySpark -> Data Lake + Pub/Sub alerts).

What this job does:
- Pulls transaction envelope events from a Pub/Sub subscription in micro-batches.
- Parses payloads, enforces schema, and engineers fraud features per micro-batch.
- Loads a pre-trained Spark PipelineModel and scores each transaction with `fraud_score`.
- Applies alert rules (model score, high transfer amount, and velocity in 5-minute buckets).
- Writes raw, scored, and alert datasets to bronze/silver/gold parquet paths.
- Publishes alert envelope events to a Pub/Sub topic for downstream consumers.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from time import perf_counter
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from google.cloud import pubsub_v1
from google.api_core import exceptions as gcloud_exceptions
from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event_envelope(
    *,
    payload: dict[str, Any],
    event_type: str,
    source: str,
    event_id: str | None = None,
    emitted_at_utc: str | None = None,
    schema_version: str = "1.0",
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "source": source,
        "emitted_at_utc": emitted_at_utc or utc_now_iso(),
        "payload": payload,
    }


def serialize_envelope(envelope: dict[str, Any]) -> bytes:
    return json.dumps(envelope, separators=(",", ":"), default=str).encode("utf-8")


def deserialize_envelope(raw: bytes) -> dict[str, Any]:
    decoded = json.loads(raw.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("Envelope must be a JSON object")
    if "payload" not in decoded or not isinstance(decoded["payload"], dict):
        raise ValueError("Envelope must include object payload")
    return decoded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cloud-native PySpark Pub/Sub fraud scoring stream")
    parser.add_argument(
        "--pubsub-project-id",
        default=os.getenv("GCP_PROJECT_ID", os.getenv("PUBSUB_PROJECT_ID", "")),
        help="GCP project id used when subscription/topic names are not fully qualified",
    )
    parser.add_argument(
        "--input-subscription",
        default=os.getenv("TRANSACTIONS_PULL_SUBSCRIPTION", ""),
        help="Pub/Sub subscription name or full path for transaction input",
    )
    parser.add_argument(
        "--alerts-topic",
        default=os.getenv("PUBSUB_FRAUD_ALERTS_TOPIC", "fraud_alerts"),
        help="Pub/Sub topic id for fraud alerts",
    )
    parser.add_argument("--model-path", default="ml/artifacts/fraud_rf_pipeline", help="Path to Spark pre-trained PipelineModel")
    parser.add_argument("--datalake-raw-path", default="data/lake/bronze/transactions_raw", help="Raw records data lake path")
    parser.add_argument("--datalake-scored-path", default="data/lake/silver/scored_transactions", help="Scored records data lake path")
    parser.add_argument("--datalake-alerts-path", default="data/lake/gold/fraud_alerts", help="Alert records data lake path")
    parser.add_argument("--fraud-score-threshold", type=float, default=0.8, help="Threshold for high fraud score")
    parser.add_argument("--high-amount-threshold", type=float, default=200000.0, help="Amount threshold for transfer/cash-out alert rule")
    parser.add_argument("--velocity-threshold", type=int, default=5, help="Minimum velocity_5min to trigger alert rule")
    parser.add_argument("--trigger-seconds", type=int, default=10, help="Loop sleep interval when no messages are available")
    parser.add_argument("--pull-max-messages", type=int, default=1000, help="Max Pub/Sub messages to pull per micro-batch")
    parser.add_argument("--pull-timeout-seconds", type=float, default=15.0, help="Pub/Sub pull timeout")
    parser.add_argument("--ack-on-error", action="store_true", help="Acknowledge messages even when processing fails")
    parser.add_argument("--shuffle-partitions", type=int, default=8, help="Minimum number of shuffle partitions for Spark operations")
    parser.add_argument(
        "--output-partitions",
        type=int,
        default=1,
        help="Maximum number of parquet files to create per output path for each micro-batch",
    )
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


def engineer_features(df: DataFrame, shuffle_partitions: int) -> DataFrame:
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

    with_time_bucket = (
        typed_df.withColumn("time_bucket_5min", F.floor(F.unix_timestamp("event_ts") / F.lit(300)).cast("long"))
        .repartition(shuffle_partitions, "nameOrig")
    )

    velocity_window = Window.partitionBy("nameOrig", "time_bucket_5min")
    featured_df = (
        with_time_bucket.withColumn("velocity_5min", F.count(F.lit(1)).over(velocity_window).cast(DoubleType()))
        .withColumn(
            "balance_change_ratio",
            F.when(F.col("oldbalanceOrg") > 0, F.col("amount") / F.col("oldbalanceOrg")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "is_new_merchant",
            F.when((F.col("oldbalanceDest") == 0.0) & (F.col("newbalanceDest") == 0.0), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .withColumn("origin_balance_delta", F.col("oldbalanceOrg") - F.col("newbalanceOrig"))
        .withColumn("dest_balance_delta", F.col("newbalanceDest") - F.col("oldbalanceDest"))
    )
    return featured_df


def build_spark(
    app_name: str,
    gcs_enabled: bool,
    gcs_credentials_file: str | None,
    shuffle_partitions: int = 8,
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    builder = (
        builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.streaming.backpressure.enabled", "true")
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


def build_subscription_path(args: argparse.Namespace, subscriber: pubsub_v1.SubscriberClient) -> str:
    subscription = args.input_subscription.strip()
    if subscription.startswith("projects/"):
        return subscription
    if not args.pubsub_project_id:
        raise ValueError("Set --pubsub-project-id when --input-subscription is not a full resource path")
    return subscriber.subscription_path(args.pubsub_project_id, subscription)


def build_topic_path(args: argparse.Namespace, publisher: pubsub_v1.PublisherClient) -> str:
    topic = args.alerts_topic.strip()
    if topic.startswith("projects/"):
        return topic
    if not args.pubsub_project_id:
        raise ValueError("Set --pubsub-project-id when --alerts-topic is not a full resource path")
    return publisher.topic_path(args.pubsub_project_id, topic)


def messages_to_dataframe(spark: SparkSession, messages: list[pubsub_v1.types.ReceivedMessage]) -> DataFrame:
    records: list[dict[str, str]] = []
    for msg in messages:
        envelope = deserialize_envelope(msg.message.data)
        payload = envelope.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        records.append(
            {
                "source_topic": msg.message.attributes.get("source", "pubsub.subscription"),
                "source_partition": "0",
                "source_offset": msg.message.message_id,
                "kafka_ingest_ts": msg.message.publish_time.isoformat() if msg.message.publish_time else "",
                "step": str(payload.get("step", "")),
                "type": str(payload.get("type", "")),
                "amount": str(payload.get("amount", "")),
                "nameOrig": str(payload.get("nameOrig", "")),
                "oldbalanceOrg": str(payload.get("oldbalanceOrg", "")),
                "newbalanceOrig": str(payload.get("newbalanceOrig", "")),
                "nameDest": str(payload.get("nameDest", "")),
                "oldbalanceDest": str(payload.get("oldbalanceDest", "")),
                "newbalanceDest": str(payload.get("newbalanceDest", "")),
                "event_emitted_at_utc": str(payload.get("event_emitted_at_utc", envelope.get("emitted_at_utc", ""))),
            }
        )
    schema = StructType(
        [
            StructField("source_topic", StringType(), True),
            StructField("source_partition", StringType(), True),
            StructField("source_offset", StringType(), True),
            StructField("kafka_ingest_ts", StringType(), True),
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
    return spark.createDataFrame(records, schema=schema)


def main() -> None:
    args = parse_args()

    if args.pull_max_messages < 1:
        raise ValueError("--pull-max-messages must be >= 1")
    if args.shuffle_partitions < 1:
        raise ValueError("--shuffle-partitions must be >= 1")
    if args.output_partitions < 1:
        raise ValueError("--output-partitions must be >= 1")
    if not args.input_subscription.strip():
        raise ValueError("Set --input-subscription to a Pub/Sub subscription name or full path")
    uses_gcs = any(
        is_cloud_uri(path)
        for path in [args.model_path, args.datalake_raw_path, args.datalake_scored_path, args.datalake_alerts_path]
    )
    gcs_credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    if not is_cloud_uri(args.model_path) and not Path(args.model_path).exists():
        raise FileNotFoundError(
            f"Pre-trained model not found: {args.model_path}. Train first with: spark-submit ml/train_fraud_model.py"
        )

    for target_path in [args.datalake_raw_path, args.datalake_scored_path, args.datalake_alerts_path]:
        if not is_cloud_uri(target_path):
            Path(target_path).mkdir(parents=True, exist_ok=True)

    spark = build_spark(
        app_name="fraud-streaming-pubsub-pyspark",
        gcs_enabled=uses_gcs,
        gcs_credentials_file=gcs_credentials_file or None,
        shuffle_partitions=args.shuffle_partitions,
    )

    default_parallelism = max(1, spark.sparkContext.defaultParallelism)
    effective_shuffle_partitions = max(args.shuffle_partitions, default_parallelism)
    spark.conf.set("spark.sql.shuffle.partitions", str(effective_shuffle_partitions))

    print(
        (
            "Spark tuning: "
            f"default_parallelism={default_parallelism} "
            f"configured_shuffle_partitions={args.shuffle_partitions} "
            f"effective_shuffle_partitions={effective_shuffle_partitions}"
        ),
        flush=True,
    )

    model = PipelineModel.load(args.model_path)
    subscriber = pubsub_v1.SubscriberClient()
    publisher = pubsub_v1.PublisherClient()
    input_subscription_path = build_subscription_path(args, subscriber)
    alerts_topic_path = build_topic_path(args, publisher)
    batch_id = 0

    def process_batch(batch_df: DataFrame, current_batch_id: int) -> int:
        batch_start = perf_counter()
        transform_plan_start = perf_counter()

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

        featured_df = engineer_features(batch_df, effective_shuffle_partitions)
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

        transform_plan_seconds = perf_counter() - transform_plan_start

        final_df = final_df.persist(StorageLevel.MEMORY_AND_DISK)
        alerts_df: DataFrame | None = None
        alert_count = 0
        try:
            alerts_df = final_df.filter(F.col("is_alert")).persist(StorageLevel.MEMORY_AND_DISK)

            alerts_collect_start = perf_counter()
            for row in alerts_df.select(*write_cols).toLocalIterator():
                alert_payload = row.asDict(recursive=True)
                envelope = build_event_envelope(
                    payload=alert_payload,
                    event_type="fraud.alert",
                    source="streaming.pyspark_fraud_streaming",
                )
                publisher.publish(
                    alerts_topic_path,
                    serialize_envelope(envelope),
                    event_type=envelope["event_type"],
                    source=envelope["source"],
                    event_id=envelope["event_id"],
                )
                alert_count += 1
            time_to_pubsub_alert_seconds = perf_counter() - alerts_collect_start

            compacted_output_partitions = min(args.output_partitions, max(1, final_df.rdd.getNumPartitions()))

            raw_write_start = perf_counter()
            final_df.select(*raw_write_cols).coalesce(compacted_output_partitions).write.mode("append").parquet(
                args.datalake_raw_path
            )
            raw_write_seconds = perf_counter() - raw_write_start

            scored_write_start = perf_counter()
            final_df.select(*write_cols).coalesce(compacted_output_partitions).write.mode("append").parquet(
                args.datalake_scored_path
            )
            scored_write_seconds = perf_counter() - scored_write_start

            alerts_write_start = perf_counter()
            if alert_count > 0:
                alerts_df.select(*write_cols).coalesce(compacted_output_partitions).write.mode("append").parquet(
                    args.datalake_alerts_path
                )
            alerts_write_seconds = perf_counter() - alerts_write_start

            batch_duration_seconds = perf_counter() - batch_start
            print(
                (
                    f"Batch {current_batch_id}: timings "
                    f"transform_plan_seconds={transform_plan_seconds:.2f} "
                    f"time_to_pubsub_alert_seconds={time_to_pubsub_alert_seconds:.2f} "
                    f"raw_write_seconds={raw_write_seconds:.2f} "
                    f"scored_write_seconds={scored_write_seconds:.2f} "
                    f"alerts_write_seconds={alerts_write_seconds:.2f} "
                    f"total_batch_seconds={batch_duration_seconds:.2f} "
                    f"alerts_published={alert_count} "
                    f"raw={args.datalake_raw_path} "
                    f"scored={args.datalake_scored_path} "
                    f"alerts={args.datalake_alerts_path} "
                    f"pubsub_topic={alerts_topic_path}"
                ),
                flush=True,
            )
        finally:
            if alerts_df is not None:
                alerts_df.unpersist()
            final_df.unpersist()

    print(
        (
            "Fraud Pub/Sub streaming job started "
            f"subscription={input_subscription_path} alerts_topic={alerts_topic_path}"
        ),
        flush=True,
    )

    while True:
        try:
            response = subscriber.pull(
                request={"subscription": input_subscription_path, "max_messages": args.pull_max_messages},
                timeout=args.pull_timeout_seconds,
            )
        except (gcloud_exceptions.DeadlineExceeded, gcloud_exceptions.ServiceUnavailable):
            # Transient pull timeouts/unavailability should not terminate long-running streaming jobs.
            continue
        received_messages = list(response.received_messages)
        if not received_messages:
            time.sleep(max(float(args.trigger_seconds), 1.0))
            continue

        ack_ids: list[str] = []
        try:
            batch_df = messages_to_dataframe(spark, received_messages)
            process_batch(batch_df, batch_id)
            batch_id += 1
            ack_ids = [message.ack_id for message in received_messages]
        except Exception:
            if args.ack_on_error:
                ack_ids = [message.ack_id for message in received_messages]
            else:
                raise

        if ack_ids:
            subscriber.acknowledge(request={"subscription": input_subscription_path, "ack_ids": ack_ids})


if __name__ == "__main__":
    main()
