# End-to-End Runbook (Local)

This runbook executes the project locally without GCS/BigQuery dependencies.

## 1) Setup

1. Complete shared setup in `docs/prerequisites.md`.
2. Prepare dataset using `data/README.md`.

## 2) Start Kafka

```bash
./scripts/kafka/kafka_up.sh
./scripts/kafka/kafka_topic_create.sh transactions_raw
./scripts/kafka/kafka_topic_create.sh fraud_alerts
```

## 3) Produce Transaction Events

```bash
python3 simulator/kafka/kafka_csv_producer.py \
  --input data/transaction_log.csv \
  --bootstrap-servers localhost:9092 \
  --topic transactions_raw \
  --interval-min 0.3 \
  --interval-max 1.0
```

## 4) Train Baseline Model

```bash
spark-submit ml/train_fraud_model.py \
  --input data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline
```

## 5) Run Streaming Scoring

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

## 6) Run Alert Consumer (Optional)

```bash
python3 consumers/alert_email_consumer.py \
  --bootstrap-servers localhost:9092 \
  --topic fraud_alerts \
  --group-id fraud-alerts-email-consumer \
  --email-use-tls
```

## 7) Run Hourly Batch Locally (Optional)

```bash
spark-submit batch/hourly_batch_processing.py \
  --silver-path data/lake/silver/scored_transactions \
  --labels-csv data/transaction_log.csv \
  --output-base data/lake/gold/hourly_batch
```

## 8) Run Daily Model Refresh Locally (Optional)

```bash
spark-submit batch/daily_model_refresh.py \
  --silver-path data/lake/silver/scored_transactions \
  --labels-csv data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline
```

## 9) Stop Kafka

```bash
./scripts/kafka/kafka_down.sh
```
