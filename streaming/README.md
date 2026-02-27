# Streaming ML Scoring (PySpark)

This module implements roadmap Step 3 and Step 5 with PySpark.

## What it does

- Reads raw JSON transactions from Kafka topic `transactions_raw`.
- Converts JSON to DataFrame and enforces schema/data types.
- Handles missing values.
- Creates engineered features: `velocity_5min`, `balance_change_ratio`, `is_new_merchant`, `origin_balance_delta`, `dest_balance_delta`.
- Loads a pre-trained Spark ML `PipelineModel`.
- Scores each transaction and adds:
  - `fraud_score`
  - `predicted_is_fraud` (true/false)
- Applies business rules and computes `is_alert`.
- Writes raw transactions (Bronze), scored data (Silver), and alerts (Gold) to local paths or GCS paths (`gs://...`).
- Publishes scored transactions and alert transactions to Kafka topics:
  - `scored-transactions`
  - `fraud-alerts`
- Optionally sends email alert summary when `fraud_score` exceeds threshold.

## Prerequisites

1. Kafka is running and topic `transactions_raw` receives events.
2. Python environment is active:

```bash
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

3. Spark is installed locally, and `spark-submit` is available.
4. For GCS output (Step 5), service account key is available locally:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
```

## 1) Train baseline model artifact

The streaming job loads a pre-trained model from `ml/artifacts/fraud_rf_pipeline`.

```bash
spark-submit ml/train_fraud_model.py \
  --input data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline
```

## 2) Create Kafka output topics

```bash
./scripts/kafka/kafka_topic_create.sh scored-transactions
./scripts/kafka/kafka_topic_create.sh fraud-alerts
```

## 3) Run streaming scoring job (local lake)

Use `spark-submit` with Kafka connector package:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  streaming/pyspark_fraud_streaming.py \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions_raw \
  --scored-topic scored-transactions \
  --alerts-topic fraud-alerts \
  --model-path ml/artifacts/fraud_rf_pipeline \
  --fraud-score-threshold 0.80
```

## Optional email alerts

Add these options when running the streaming job:

```bash
--enable-email-alerts \
--smtp-host smtp.example.com \
--smtp-port 587 \
--smtp-user user@example.com \
--smtp-password '<password>' \
--email-from alerts@example.com \
--email-to oncall@example.com \
--email-use-tls
```

## 4) Read processed data from data lake

Preview scored transactions:

```bash
spark-submit streaming/read_datalake_sample.py \
  --path data/lake/silver/scored_transactions \
  --show-schema \
  --limit 20
```

Preview alert-only records:

```bash
spark-submit streaming/read_datalake_sample.py \
  --path data/lake/gold/fraud_alerts \
  --only-alerts \
  --limit 20
```

## Notes

- `velocity_5min` is computed per sender (`nameOrig`) within each 5-minute bucket.
- Kafka connector package **must** match your local Spark + Scala versions. Check with:
  - `spark-submit --version`
  - Example for Spark 4.1.1 + Scala 2.13: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1`
- Scored and alert records are written locally by default:
  - `data/lake/bronze/transactions_raw`
  - `data/lake/silver/scored_transactions`
  - `data/lake/gold/fraud_alerts`

## 5) Run streaming scoring job with GCS (Step 5)

After Terraform apply, use the output bucket names and write directly to GCS:

- Bronze bucket: keep raw/landing and checkpoints
- Silver bucket: scored transaction parquet output
- Gold bucket: alert-only parquet output

Example command:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  streaming/pyspark_fraud_streaming.py \
  --bootstrap-servers localhost:9092 \
  --input-topic transactions_raw \
  --scored-topic scored-transactions \
  --alerts-topic fraud-alerts \
  --model-path ml/artifacts/fraud_rf_pipeline \
  --checkpoint-dir gs://<bronze_bucket>/checkpoints/fraud_stream \
  --datalake-raw-path gs://<bronze_bucket>/raw/transactions_raw \
  --datalake-scored-path gs://<silver_bucket>/scored_transactions \
  --datalake-alerts-path gs://<gold_bucket>/fraud_alerts \
  --fraud-score-threshold 0.80
```

If checkpoint startup fails with `CANNOT_LOAD_CHECKPOINT_FILE_MANAGER` on a `gs://` path, verify:

- `GOOGLE_APPLICATION_CREDENTIALS` points to your service account key file.
- `gcs-connector` is present in `--packages`.

Preview from GCS:

```bash
spark-submit \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  streaming/read_datalake_sample.py \
  --path gs://<bronze_bucket>/raw/transactions_raw \
  --show-schema \
  --limit 20

spark-submit \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  streaming/read_datalake_sample.py \
  --path gs://<silver_bucket>/scored_transactions \
  --show-schema \
  --limit 20

spark-submit \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  streaming/read_datalake_sample.py \
  --path gs://<gold_bucket>/fraud_alerts \
  --only-alerts \
  --limit 20
```
