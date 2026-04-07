# End-to-End Runbook (Cloud-Native GCP)

This runbook executes the cloud-native path on GCP using Pub/Sub, Dataproc Serverless, Dataform, and Cloud Composer.

Legacy local/Docker behavior is preserved only in the frozen `v1-onprem` version line and is not part of this execution path.

## 1) One-Time Setup

Complete shared one-time setup in [prerequisites.md](prerequisites.md).

Then verify the source dataset exists:

- `data/transaction_log.csv`

If you are provisioning fresh cloud infrastructure, use [../infra/terraform/README.md](../infra/terraform/README.md) before running the rest of this runbook.

## 2) Provision GCP Foundation

Provision infrastructure with Terraform:

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
cd ../..
```

Confirm the Terraform outputs for:

- GCS bucket names used for lake, checkpoints, and artifacts
- BigQuery dataset reference used by the cloud runtime

## 3) Set Cloud Runtime Variables

Run these exports once per shell session before any Dataproc, Pub/Sub, BigQuery, Dataform, or Composer commands:

```bash
export GCP_PROJECT_ID="<your-project-id>"
export GCP_REGION="us-central1"
export GCP_GCS_BUCKET="<your-lake-and-artifacts-bucket>"
export GCP_DATAPROC_DEPS_BUCKET="${GCP_GCS_BUCKET}"
export SA_NAME="dataproc-fraud-runtime"
export GCP_DATAPROC_SERVICE_ACCOUNT="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
export GCP_DATAPROC_SUBNET="projects/${GCP_PROJECT_ID}/regions/${GCP_REGION}/subnetworks/<subnet-name>"

export RUN_TS="$(date +%Y%m%d%H%M%S)"
export GCP_DATAPROC_HOURLY_BATCH_ID="fraud-hourly-batch-${RUN_TS}"
export GCP_DATAPROC_BQ_LOAD_BATCH_ID="fraud-hourly-bq-load-${RUN_TS}"
export GCP_DATAPROC_DAILY_BATCH_ID="fraud-daily-model-refresh-${RUN_TS}"
export GCP_DATAPROC_STREAMING_BATCH_ID="fraud-streaming-batch-${RUN_TS}"

export PIPELINE_MODE="gcp-native"
export FRAUD_SHUFFLE_PARTITIONS="8"

export FRAUD_SILVER_PATH="gs://${GCP_GCS_BUCKET}/lake/silver/scored_transactions"
export FRAUD_LABELS_CSV="gs://${GCP_GCS_BUCKET}/inputs/transaction_log.csv"
export FRAUD_HOURLY_OUTPUT_BASE="gs://${GCP_GCS_BUCKET}/lake/gold/hourly_batch"
export FRAUD_MODEL_OUTPUT="gs://${GCP_GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline"
export FRAUD_BQ_DATASET="fraud_analytics"
export FRAUD_RETRAINING_TABLE="retraining_dataset"

export FRAUD_INPUT_SUBSCRIPTION="transactions-raw-pull-sub"
export FRAUD_ALERTS_TOPIC="fraud_alerts"

export DATAFORM_REGION="${GCP_REGION}"
export DATAFORM_REPOSITORY_ID="fraud-warehouse"
export DATAFORM_RUN_WORKFLOW_CONFIG_ID="fraud-main"
export DATAFORM_ASSERT_WORKFLOW_CONFIG_ID="fraud-assertions"

export COMPOSER_PROJECT_ID="${GCP_PROJECT_ID}"
export COMPOSER_REGION="${GCP_REGION}"
export COMPOSER_ENV_NAME="fraud-orchestrator"
export FRAUD_HOURLY_BATCH_PY_URI="gs://${GCP_GCS_BUCKET}/code/batch/hourly_batch_processing.py"
export FRAUD_HOURLY_BQ_LOAD_PY_URI="gs://${GCP_GCS_BUCKET}/code/batch/load_hourly_batch_to_bigquery.py"
export FRAUD_DAILY_MODEL_REFRESH_PY_URI="gs://${GCP_GCS_BUCKET}/code/batch/daily_model_refresh.py"
```

Initialize the gcloud context:

```bash
gcloud config set project "$GCP_PROJECT_ID"
gcloud config set dataproc/region "$GCP_REGION"
```

## 4) Enable GCP Services and Grant IAM

Enable the required APIs:

```bash
gcloud services enable dataproc.googleapis.com bigquery.googleapis.com storage.googleapis.com pubsub.googleapis.com dataform.googleapis.com composer.googleapis.com
```

Create and grant the Dataproc runtime service account:

```bash
gcloud iam service-accounts create "$SA_NAME" \
  --project "$GCP_PROJECT_ID" \
  --display-name "Dataproc Fraud Runtime"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member "serviceAccount:${GCP_DATAPROC_SERVICE_ACCOUNT}" \
  --role "roles/dataproc.worker"

gcloud storage buckets add-iam-policy-binding "gs://${GCP_GCS_BUCKET}" \
  --member "serviceAccount:${GCP_DATAPROC_SERVICE_ACCOUNT}" \
  --role "roles/storage.objectAdmin"
```

If you are using a dedicated Pub/Sub identity for publishing and consuming alerts, grant it the needed roles as well:

```bash
gcloud pubsub subscriptions add-iam-policy-binding "$FRAUD_INPUT_SUBSCRIPTION" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${GCP_DATAPROC_SERVICE_ACCOUNT}" \
  --role="roles/pubsub.subscriber"

gcloud pubsub topics add-iam-policy-binding "$FRAUD_ALERTS_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${GCP_DATAPROC_SERVICE_ACCOUNT}" \
  --role="roles/pubsub.publisher"
```

Grant BigQuery access for the daily refresh job:

```bash
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member "serviceAccount:${GCP_DATAPROC_SERVICE_ACCOUNT}" \
  --role "roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member "serviceAccount:${GCP_DATAPROC_SERVICE_ACCOUNT}" \
  --role "roles/bigquery.readSessionUser"
```

## 5) Set Up Pub/Sub Messaging

Create the topics and subscriptions:

```bash
gcloud pubsub topics create "$FRAUD_ALERTS_TOPIC"
gcloud pubsub topics create "transactions_raw"

gcloud pubsub subscriptions create "$FRAUD_INPUT_SUBSCRIPTION" \
  --topic="transactions_raw"

gcloud pubsub subscriptions create "fraud-alerts-pull-sub" \
  --topic="$FRAUD_ALERTS_TOPIC"
```

Optional inspection:

```bash
gcloud pubsub topics list
gcloud pubsub subscriptions describe "$FRAUD_INPUT_SUBSCRIPTION"
```

Run the local alert consumer in a dedicated terminal and keep it running:

```bash
python3 consumers/alert_pubsub_consumer.py \
  --pubsub-project-id "$GCP_PROJECT_ID" \
  --pubsub-subscription "fraud-alerts-pull-sub" \
  --delivery-mode email \
  --email-use-tls
```

If you are testing delivery manually, publish a smoke-test alert:

```bash
gcloud pubsub topics publish "$FRAUD_ALERTS_TOPIC" \
  --message='{"schema_version":"1.0","event_id":"manual-test-1","event_type":"fraud.alert","source":"manual.test","emitted_at_utc":"2026-01-01T00:00:00Z","payload":{"type":"TRANSFER","nameOrig":"C_TEST","nameDest":"C_DEST","amount":99999.99,"fraud_score":0.99,"predicted_is_fraud":true,"is_alert":true,"event_ts":"2026-01-01T00:00:00Z"}}' \
  --attribute=event_type=fraud.alert,source=manual.test,event_id=manual-test-1
```

## 6) Publish Transaction Events to Pub/Sub

Use the repository publisher to validate the transaction ingest path:

```bash
python3 simulator/pubsub/pubsub_csv_publisher.py \
  --input data/transaction_log.csv \
  --project-id "$GCP_PROJECT_ID" \
  --topic transactions_raw \
  --interval-min 0.1 \
  --interval-max 0.4 \
  --max-events 100
```

## 7) Train or Upload the Model Artifact

If the trained Spark model does not exist in GCS yet, build it locally and upload it:

```bash
python3 ml/train_fraud_model.py \
  --input data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline

gsutil -m cp -r ml/artifacts/fraud_rf_pipeline "$FRAUD_MODEL_OUTPUT"
```

If the model was trained with a Spark version that is incompatible with Dataproc, rebuild it on Dataproc Serverless with:

```bash
bash scripts/gcp/dataproc/submit_model_train_bootstrap.sh
```

## 8) Run the Streaming Dataproc Batch

Run the streaming job after the model artifact is present in GCS:

```bash
bash scripts/gcp/dataproc/submit_streaming_batch.sh
```

This batch consumes from Pub/Sub and writes the raw, scored, and alert outputs to GCS.

If you see duplicate run collisions, make sure only one streaming batch is active for the same output prefixes.

## 9) Run the Hourly Dataproc Batch and Load to BigQuery

Run the hourly batch first:

```bash
bash scripts/gcp/dataproc/submit_hourly_batch.sh
```

Optional targeted replay for one UTC hour:

```bash
export FRAUD_TARGET_HOUR_UTC="2026-04-07-13"
bash scripts/gcp/dataproc/submit_hourly_batch.sh
```

Then load the hourly parquet outputs to BigQuery:

```bash
bash scripts/gcp/dataproc/submit_hourly_bq_load.sh
```

## 10) Run the Daily Model Refresh

Once the hourly outputs are available, run the daily refresh:

```bash
bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
```

If Dataproc Serverless quota is tight, you can override the Spark properties used for the job:

```bash
export GCP_DATAPROC_SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.driver.cores=4,spark.executor.instances=2,spark.executor.cores=4"
bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
```

If you need CSV fallback instead of BigQuery, switch the training source to CSV before rerunning the job.

## 11) Run Dataform and Validate Parity

Use the Google Cloud Console Dataform UI for repository setup, release configuration, and workflow invocation.

At minimum, confirm:

1. Dataform repository is connected to this Git repository at the repository root.
2. Execution service account has the required BigQuery permissions.
3. Release and workflow configs point to the cloud-native dataset and schema names.
4. The workflow run succeeds for the first conversion wave.

For parity checks, compare the Dataform outputs against the existing dbt outputs for:

1. Row counts
2. Hourly KPI metrics
3. Required columns and nullability

Recommended acceptance thresholds:

- Row-count delta per hourly table: <= 0.5%
- KPI delta on fraud rate: <= 1% relative difference
- Zero missing required columns

## 12) Deploy and Run Composer Orchestration

Upload the Composer DAG inputs to GCS:

```bash
gsutil cp batch/hourly_batch_processing.py "$FRAUD_HOURLY_BATCH_PY_URI"
gsutil cp batch/load_hourly_batch_to_bigquery.py "$FRAUD_HOURLY_BQ_LOAD_PY_URI"
gsutil cp batch/daily_model_refresh.py "$FRAUD_DAILY_MODEL_REFRESH_PY_URI"
```

Set the Composer Airflow variables and sync the DAGs:

```bash
bash scripts/gcp/composer/set_composer_airflow_variables.sh
bash scripts/gcp/composer/sync_dags_to_composer.sh
```

Then trigger the DAGs from the Composer environment:

```bash
gcloud composer environments run "$COMPOSER_ENV_NAME" \
  --location "$COMPOSER_REGION" \
  --project "$COMPOSER_PROJECT_ID" \
  dags trigger -- fraud_hourly_batch_and_warehouse

gcloud composer environments run "$COMPOSER_ENV_NAME" \
  --location "$COMPOSER_REGION" \
  --project "$COMPOSER_PROJECT_ID" \
  dags trigger -- fraud_daily_model_refresh
```

## 13) Verify Outputs and Dashboard

Check the hourly outputs in GCS:

```bash
gsutil ls "$FRAUD_HOURLY_OUTPUT_BASE/curated_scored"
gsutil ls "$FRAUD_HOURLY_OUTPUT_BASE/retraining_dataset"
gsutil ls "$FRAUD_HOURLY_OUTPUT_BASE/monitoring_hourly"
```

Check the refreshed model artifact:

```bash
gsutil ls "$FRAUD_MODEL_OUTPUT"
```

Inspect Dataproc batch status:

```bash
gcloud dataproc batches list --region "$GCP_REGION"
gcloud dataproc batches describe "$GCP_DATAPROC_HOURLY_BATCH_ID" --region "$GCP_REGION"
gcloud dataproc batches describe "$GCP_DATAPROC_DAILY_BATCH_ID" --region "$GCP_REGION"
```

Confirm the BigQuery warehouse tables are present before opening Tableau:

- `fraud_analytics.mart_fraud_hourly_kpis`

Dashboard guide: [tableau-chart-instructions.md](tableau-chart-instructions.md)

## 14) Cleanup and Troubleshooting

Dataproc batches are managed services and usually do not need manual cleanup after completion.

If you are resetting a sandbox environment, remove only the cloud resources you created:

```bash
gcloud pubsub subscriptions delete "fraud-alerts-pull-sub"
gcloud pubsub subscriptions delete "$FRAUD_INPUT_SUBSCRIPTION"
gcloud pubsub topics delete "$FRAUD_ALERTS_TOPIC"
gcloud pubsub topics delete "transactions_raw"
```

Common checks:

1. If `GOOGLE_APPLICATION_CREDENTIALS` is missing for local GCS access, set it only for local runs that need direct file access.
2. If the daily refresh fails with a BigQuery connector classpath error, remove any manual connector override and rerun the job.
3. If the streaming batch fails with a Pub/Sub permission error, recheck the runtime service account bindings.
4. If Composer DAGs do not appear, confirm the DAG files were synced to the Composer DAG bucket and that the environment variables are set.