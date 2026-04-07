#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to your GCP project id}"
REGION="${GCP_REGION:-us-central1}"
BATCH_ID="${GCP_DATAPROC_BQ_LOAD_BATCH_ID:-fraud-hourly-bq-load-$(date +%Y%m%d%H%M%S)}"
GCS_BUCKET="${GCP_GCS_BUCKET:?Set GCP_GCS_BUCKET to the bucket that stores lake and input data}"
DEPS_BUCKET="${GCP_DATAPROC_DEPS_BUCKET:-$GCS_BUCKET}"
DATASET="${FRAUD_BQ_DATASET:-fraud_analytics}"
GCS_OUTPUT_BASE="${FRAUD_HOURLY_OUTPUT_BASE:-gs://${GCS_BUCKET}/lake/gold/hourly_batch}"
WRITE_DISPOSITION="${FRAUD_BQ_LOAD_WRITE_DISPOSITION:-WRITE_TRUNCATE}"
TEMP_GCS_BUCKET="${FRAUD_BQ_TEMP_BUCKET:-$GCS_BUCKET}"
TABLES="${FRAUD_BQ_LOAD_TABLES:-}"
EXTRA_SPARK_PROPERTIES="${GCP_DATAPROC_SPARK_PROPERTIES:-}"

script_args=(
  "--runtime-mode" "gcp-native"
  "--project-id" "$PROJECT_ID"
  "--dataset" "$DATASET"
  "--gcs-output-base" "$GCS_OUTPUT_BASE"
  "--write-disposition" "$WRITE_DISPOSITION"
  "--create-dataset-if-missing"
)

if [[ -n "$TEMP_GCS_BUCKET" ]]; then
  script_args+=("--temporary-gcs-bucket" "$TEMP_GCS_BUCKET")
fi

if [[ -n "$TABLES" ]]; then
  # FRAUD_BQ_LOAD_TABLES example: "curated_scored retraining_dataset"
  read -r -a tables_array <<< "$TABLES"
  script_args+=("--tables" "${tables_array[@]}")
fi

batch_flags=(
  --project="$PROJECT_ID"
  --region="$REGION"
  --deps-bucket="$DEPS_BUCKET"
)

if [[ -n "$EXTRA_SPARK_PROPERTIES" ]]; then
  batch_flags+=("--properties=${EXTRA_SPARK_PROPERTIES}")
fi

# Dataproc batch ids must be unique per region; auto-suffix on collision for safe reruns.
if gcloud dataproc batches describe "$BATCH_ID" --project="$PROJECT_ID" --region="$REGION" >/dev/null 2>&1; then
  BATCH_ID="${BATCH_ID}-retry-$(date +%Y%m%d%H%M%S)"
fi

batch_flags+=("--batch=${BATCH_ID}")

if [[ -n "${GCP_DATAPROC_SERVICE_ACCOUNT:-}" ]]; then
  batch_flags+=("--service-account=${GCP_DATAPROC_SERVICE_ACCOUNT}")
fi

if [[ -n "${GCP_DATAPROC_SUBNET:-}" ]]; then
  batch_flags+=("--subnet=${GCP_DATAPROC_SUBNET}")
fi

gcloud dataproc batches submit pyspark "$REPO_ROOT/batch/load_hourly_batch_to_bigquery.py" "${batch_flags[@]}" -- "${script_args[@]}"
