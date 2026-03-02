# End-to-End Runbook (GCP)

This runbook follows roadmap steps for GCS + BigQuery execution.

## 1) One-Time Setup

From project root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m pip install -r dbt/requirements.txt
```

Make helper scripts executable:

```bash
chmod +x scripts/kafka/kafka_up.sh scripts/kafka/kafka_down.sh scripts/kafka/kafka_topics.sh scripts/kafka/kafka_topic_create.sh
chmod +x scripts/airflow/airflow_up.sh scripts/airflow/airflow_down.sh
```

Prepare dataset file:

- Ensure `data/transaction_log.csv` exists.

## 2) Provision GCP Foundation (Terraform)

Create service account key and place it at:

- `infra/terraform/keys/terraform-sa-key.json`

Then run Terraform:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars and set:
# - project_id
# - service_account_key_file=./keys/terraform-sa-key.json
terraform init
terraform plan
terraform apply
```

Capture outputs and export them as shell variables:

```bash
export FRAUD_GCP_PROJECT_ID="<your-gcp-project-id>"
export FRAUD_BRONZE_BUCKET="<terraform-output-bronze-bucket-name>"
export FRAUD_SILVER_BUCKET="<terraform-output-silver-bucket-name>"
export FRAUD_GOLD_BUCKET="<terraform-output-gold-bucket-name>"
export FRAUD_BQ_DATASET="fraud_analytics"
```

Go back to project root:

```bash
cd ../..
```

## 3) Export GCP Credentials

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
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
   --interval-max 1.0
```

## 6) Train Baseline Model Artifact

```bash
spark-submit ml/train_fraud_model.py \
   --input data/transaction_log.csv \
   --model-output ml/artifacts/fraud_rf_pipeline
```

## 7) Run Streaming to GCS

Run in another terminal:

```bash
source .venv/bin/activate
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"

spark-submit \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
   streaming/pyspark_fraud_streaming.py \
   --bootstrap-servers localhost:9092 \
   --input-topic transactions_raw \
   --alerts-topic fraud_alerts \
   --model-path ml/artifacts/fraud_rf_pipeline \
   --checkpoint-dir gs://$FRAUD_BRONZE_BUCKET/checkpoints/fraud_stream \
   --datalake-raw-path gs://$FRAUD_BRONZE_BUCKET/raw/transactions_raw \
   --datalake-scored-path gs://$FRAUD_SILVER_BUCKET/scored_transactions \
   --datalake-alerts-path gs://$FRAUD_GOLD_BUCKET/fraud_alerts \
   --fraud-score-threshold 0.80
```

## 8) Run Alert Consumer (Optional)

```bash
source .venv/bin/activate
python3 consumers/alert_email_consumer.py \
   --bootstrap-servers localhost:9092 \
   --topic fraud_alerts \
   --group-id fraud-alerts-email-consumer \
   --email-use-tls
```

## 9) Start Airflow Orchestration (Preferred)

Prepare env file:

```bash
cp airflow/.env.example airflow/.env
```

Edit `airflow/.env` and set at least:

- `FRAUD_GCP_PROJECT_ID=$FRAUD_GCP_PROJECT_ID`
- `DBT_BIGQUERY_PROJECT=$FRAUD_GCP_PROJECT_ID`
- `FRAUD_SILVER_PATH=gs://$FRAUD_SILVER_BUCKET/scored_transactions`
- `FRAUD_BATCH_OUTPUT_BASE=gs://$FRAUD_GOLD_BUCKET/hourly_batch`
- `FRAUD_MODEL_OUTPUT=gs://$FRAUD_GOLD_BUCKET/models/fraud_rf_pipeline`

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
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"

spark-submit \
   --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
   batch/hourly_batch_processing.py \
   --silver-path gs://$FRAUD_SILVER_BUCKET/scored_transactions \
   --labels-csv data/transaction_log.csv \
   --output-base gs://$FRAUD_GOLD_BUCKET/hourly_batch \
   --model-output gs://$FRAUD_GOLD_BUCKET/models/fraud_rf_pipeline
```

### 11.2) Load Curated Outputs to BigQuery

```bash
source .venv/bin/activate
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"

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
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"

cd dbt
dbt debug
dbt deps
dbt run
dbt test
cd ..
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
