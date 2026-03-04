# End-to-End Runbook (GCP)

This runbook follows roadmap steps for GCS + BigQuery execution.

## 1) One-Time Setup

Complete shared one-time setup in [prerequisites.md](prerequisites.md).

Then verify dataset file exists:

- `data/transaction_log.csv`

## 2) Provision GCP Foundation (Terraform)

Provision infrastructure using [../infra/terraform/README.md](../infra/terraform/README.md).

Go back to project root:

```bash
cd ../..
```

## 3) Verify GCP Credentials File

```bash
GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
```

Optional quick check:

```bash
ls -l "$GOOGLE_APPLICATION_CREDENTIALS"
```

## 4) Start Kafka and Create Topics

```bash
./scripts/kafka/kafka_up.sh
./scripts/kafka/kafka_topic_create.sh transactions_raw
./scripts/kafka/kafka_topic_create.sh fraud_alerts
./scripts/kafka/kafka_topics.sh --list
```

## 5) Produce Transaction Events

Run producer in a dedicated terminal (keep running):

```bash
source .venv/bin/activate
python3 simulator/kafka/kafka_csv_producer.py \
   --input data/transaction_log.csv \
   --bootstrap-servers localhost:9092 \
   --topic transactions_raw \
   --interval-min 0.3 \
   --interval-max 1.0 \
   --max-events 100
```

## 6) Train Baseline Model Artifact

```bash
spark-submit ml/train_fraud_model.py \
   --input data/transaction_log.csv \
   --model-output ml/artifacts/fraud_rf_pipeline
```

## 6.1) Export Runtime Variables (Per New Terminal)

Run this in each new terminal before steps that use `gs://...` paths or BigQuery:

```bash
export FRAUD_GCP_PROJECT_ID="<your-gcp-project-id>"
export FRAUD_BRONZE_BUCKET="<terraform-output-bronze-bucket-name>"
export FRAUD_SILVER_BUCKET="<terraform-output-silver-bucket-name>"
export FRAUD_GOLD_BUCKET="<terraform-output-gold-bucket-name>"
export FRAUD_BQ_DATASET="fraud_analytics"
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
```

## 7) Run Alert Consumer First (Recommended)

Run in a dedicated terminal and keep it running before starting Spark streaming.

### 7.1) Configure consumer email settings

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

### 7.2) Start consumer

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

### 7.3) Optional quick smoke test before Spark

```bash
python3 consumers/publish_test_alert.py --topic fraud_alerts --count 1
```

You should see `Email sent for message 1` in the consumer terminal.

## 8) Run Streaming to GCS

Run in another terminal:

```bash
source .venv/bin/activate
# Run exports from step 6.1 in this terminal before spark-submit.

spark-submit \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
   streaming/pyspark_fraud_streaming.py \
   --bootstrap-servers localhost:9092 \
   --input-topic transactions_raw \
   --alerts-topic fraud_alerts \
   --starting-offsets earliest \
   --model-path ml/artifacts/fraud_rf_pipeline \
   --checkpoint-dir gs://$FRAUD_BRONZE_BUCKET/checkpoints/fraud_stream \
   --datalake-raw-path gs://$FRAUD_BRONZE_BUCKET/raw/transactions_raw \
   --datalake-scored-path gs://$FRAUD_SILVER_BUCKET/scored_transactions \
   --datalake-alerts-path gs://$FRAUD_GOLD_BUCKET/fraud_alerts \
   --fraud-score-threshold 0.80
```

## 9) Start Airflow Orchestration (Preferred)

Recommended: generate `airflow/.env` from Step 6.1 exported variables:

```bash
./scripts/airflow/write_airflow_env.sh
```

Quick check before starting Airflow:

```bash
grep -E 'FRAUD_GCP_PROJECT_ID|FRAUD_SILVER_PATH|FRAUD_BATCH_OUTPUT_BASE|FRAUD_MODEL_OUTPUT|FRAUD_BIGQUERY_DATASET' airflow/.env
```

Manual fallback (only if you do not use the script):

```bash
cp airflow/.env.example airflow/.env
```

Edit `airflow/.env` and set at least:

- `FRAUD_GCP_PROJECT_ID=<your real gcp project id>`
- `DBT_BIGQUERY_PROJECT=<same real gcp project id>`
- `FRAUD_BIGQUERY_DATASET=fraud_analytics`
- `FRAUD_BQ_RETRAINING_TABLE=retraining_dataset`
- `FRAUD_SILVER_PATH=gs://<your-silver-bucket>/scored_transactions`
- `FRAUD_BATCH_OUTPUT_BASE=gs://<your-gold-bucket>/hourly_batch`
- `FRAUD_MODEL_OUTPUT=gs://<your-gold-bucket>/models/fraud_rf_pipeline`

Important: in `airflow/.env`, do not keep literal `$FRAUD_...` placeholders. Values must be concrete strings.

Start Airflow:

```bash
./scripts/airflow/airflow_up.sh
```

Open UI:

- URL: `http://localhost:8080`
- User: `admin`
- Password: `admin`

Enable DAG:

- `fraud_hourly_batch_and_warehouse`
- `fraud_daily_model_refresh` (daily model retraining)

Wait for DAG completion, then continue to dashboard step.

## 10) Build Dashboard

Open Looker Studio and connect table:

- `fraud_analytics.mart_fraud_hourly_kpis`

Guide: `dashboards/README.md`

## 11) Manual Fallback (Without Airflow)

Use this only if Airflow is paused or unavailable.

### 11.1) Run Hourly Batch on GCS

```bash
source .venv/bin/activate
# Run exports from step 6.1 in this terminal before spark-submit.

spark-submit \
   --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
   batch/hourly_batch_processing.py \
   --silver-path gs://$FRAUD_SILVER_BUCKET/scored_transactions \
   --labels-csv data/transaction_log.csv \
   --output-base gs://$FRAUD_GOLD_BUCKET/hourly_batch
```

### 11.2) Load Curated Outputs to BigQuery

```bash
source .venv/bin/activate
# Run exports from step 6.1 in this terminal before python3.

python3 batch/load_hourly_batch_to_bigquery.py \
   --project-id "$FRAUD_GCP_PROJECT_ID" \
   --dataset "$FRAUD_BQ_DATASET" \
   --gcs-output-base gs://$FRAUD_GOLD_BUCKET/hourly_batch \
   --write-disposition WRITE_TRUNCATE \
   --create-dataset-if-missing
```

### 11.3) Build Warehouse Models with dbt

```bash
source .venv/bin/activate
cp dbt/profiles.yml.example dbt/profiles.yml

export DBT_PROFILES_DIR="$PWD/dbt"
export DBT_BIGQUERY_PROJECT="$FRAUD_GCP_PROJECT_ID"
export DBT_BIGQUERY_DATASET="$FRAUD_BQ_DATASET"
# Run exports from step 6.1 in this terminal before dbt commands.

cd dbt
dbt debug
dbt deps
dbt run
dbt test
cd ..
```

### 11.4) Run Daily Model Refresh Manually (Optional)

```bash
source .venv/bin/activate
# Run exports from step 6.1 in this terminal before spark-submit.

spark-submit \
   --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
   batch/daily_model_refresh.py \
   --training-source bigquery \
   --project-id "$FRAUD_GCP_PROJECT_ID" \
   --dataset "$FRAUD_BQ_DATASET" \
   --retraining-table retraining_dataset \
   --model-output gs://$FRAUD_GOLD_BUCKET/models/fraud_rf_pipeline
```

## 12) Stop Local Services

Stop Kafka:

```bash
./scripts/kafka/kafka_down.sh
```

Stop Airflow (if running):

```bash
./scripts/airflow/airflow_down.sh
```
