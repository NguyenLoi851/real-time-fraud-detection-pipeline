# Streaming ML Scoring (PySpark)

## Purpose

Consume Kafka transactions, run online fraud scoring, persist lake outputs, and publish alert events.

## Inputs and Outputs

- Input topic: `transactions_raw`
- Output topic: `fraud_alerts`
- Local lake outputs (default):
  - `data/lake/bronze/transactions_raw`
  - `data/lake/silver/scored_transactions`
  - `data/lake/gold/fraud_alerts`

## Prerequisites

Complete shared setup first: [../docs/prerequisites.md](../docs/prerequisites.md)

## Train Baseline Model

```bash
spark-submit ml/train_fraud_model.py \
  --input data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline
```

## Run (Local Lake)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  streaming/pyspark_fraud_streaming.py \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions_raw \
  --alerts-topic fraud_alerts \
  --model-path ml/artifacts/fraud_rf_pipeline \
  --fraud-score-threshold 0.80
```

## Run (GCS Lake)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  streaming/pyspark_fraud_streaming.py \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions_raw \
  --alerts-topic fraud_alerts \
  --model-path ml/artifacts/fraud_rf_pipeline \
  --checkpoint-dir gs://<bronze_bucket>/checkpoints/fraud_stream \
  --datalake-raw-path gs://<bronze_bucket>/raw/transactions_raw \
  --datalake-scored-path gs://<silver_bucket>/scored_transactions \
  --datalake-alerts-path gs://<gold_bucket>/fraud_alerts \
  --fraud-score-threshold 0.80
```

## Validate Output Samples

```bash
spark-submit streaming/read_datalake_sample.py \
  --path data/lake/silver/scored_transactions \
  --show-schema \
  --limit 20
```

## Downstream Consumers

Alert delivery is handled separately: [../consumers/README.md](../consumers/README.md)

## Troubleshooting

See shared operations guide: [../docs/operations.md](../docs/operations.md)
