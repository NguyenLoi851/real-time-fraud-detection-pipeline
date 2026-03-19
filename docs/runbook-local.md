# End-to-End Runbook (Local)

This runbook executes the project locally without GCS/BigQuery dependencies.

## 1) One-Time Setup

Complete shared one-time setup in [prerequisites.md](prerequisites.md).

Then verify dataset file exists:

- `data/transaction_log.csv`

Prepare dataset using `data/README.md` if needed.

## 2) Start Kafka and Create Topics

```bash
./scripts/kafka/kafka_up.sh
./scripts/kafka/kafka_topic_create.sh transactions_raw
./scripts/kafka/kafka_topic_create.sh fraud_alerts
./scripts/kafka/kafka_topics.sh --list
```

## 3) Produce Transaction Events

Run producer in a dedicated terminal (keep running):

```bash
source .venv/bin/activate
python3 simulator/kafka/kafka_csv_producer.py \
  --input data/transaction_log.csv \
  --bootstrap-servers localhost:9092 \
  --topic transactions_raw \
  --interval-min 0.3 \
  --interval-max 1.0
```

## 4) Train Baseline Model Artifact

```bash
spark-submit ml/train_fraud_model.py \
  --input data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline
```

## 5) Run Alert Consumer First (Recommended)

Run in a dedicated terminal and keep it running before starting Spark streaming.

### 5.1) Configure consumer email settings

```bash
cp consumers/.env.example consumers/.env
```

Edit `consumers/.env` and set at least:

- `ALERT_SMTP_HOST`
- `ALERT_SMTP_PORT` (usually `587`)
- `ALERT_SMTP_USER`
- `ALERT_SMTP_PASSWORD`
- `ALERT_EMAIL_FROM`
- `ALERT_EMAIL_TO`

### 5.2) Start consumer

```bash
source .venv/bin/activate
python3 consumers/alert_email_consumer.py \
  --bootstrap-servers localhost:9092 \
  --topic fraud_alerts \
  --group-id fraud-alerts-email-consumer \
  --email-use-tls
```

Expected startup log:

- `Email consumer listening topic='fraud_alerts' bootstrap='localhost:9092' group='fraud-alerts-email-consumer'`

If no new alerts appear during reruns, use a new group id or add `--from-beginning` for replay.

### 5.3) Optional quick smoke test before Spark

```bash
source .venv/bin/activate
python3 consumers/publish_test_alert.py --topic fraud_alerts --count 1
```

You should see `Email sent for message 1` in the consumer terminal.

## 6) Run Streaming Scoring

Run in another terminal:

```bash
source .venv/bin/activate
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  streaming/pyspark_fraud_streaming.py \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions_raw \
  --alerts-topic fraud_alerts \
  --starting-offsets earliest \
  --model-path ml/artifacts/fraud_rf_pipeline \
  --checkpoint-dir data/checkpoints/fraud_stream \
  --datalake-raw-path data/lake/bronze/transactions_raw \
  --datalake-scored-path data/lake/silver/scored_transactions \
  --datalake-alerts-path data/lake/gold/fraud_alerts \
  --trigger-seconds 10 \
  --max-offset-per-trigger 1000 \
  --shuffle-partitions 8 \
  --fraud-score-threshold 0.80
```

## 7) Run Hourly Batch Locally (Optional)

```bash
source .venv/bin/activate
spark-submit batch/hourly_batch_processing.py \
  --silver-path data/lake/silver/scored_transactions \
  --labels-csv data/transaction_log.csv \
  --output-base data/lake/gold/hourly_batch
```

## 8) Run Daily Model Refresh Locally (Optional)

```bash
source .venv/bin/activate
spark-submit batch/daily_model_refresh.py \
  --training-source csv \
  --silver-path data/lake/silver/scored_transactions \
  --labels-csv data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline
```

## 9) Stop Kafka

```bash
./scripts/kafka/kafka_down.sh
```
