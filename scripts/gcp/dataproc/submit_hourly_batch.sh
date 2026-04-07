#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to your GCP project id}"
REGION="${GCP_REGION:-us-central1}"
BATCH_ID="${GCP_DATAPROC_HOURLY_BATCH_ID:-fraud-hourly-batch-$(date +%Y%m%d%H%M%S)}"
RUNTIME_MODE="${PIPELINE_MODE:-gcp-native}"
SHUFFLE_PARTITIONS="${FRAUD_SHUFFLE_PARTITIONS:-8}"
GCS_BUCKET="${GCP_GCS_BUCKET:?Set GCP_GCS_BUCKET to the bucket that stores lake and input data}"
DEPS_BUCKET="${GCP_DATAPROC_DEPS_BUCKET:-$GCS_BUCKET}"
SILVER_PATH="${FRAUD_SILVER_PATH:-gs://${GCS_BUCKET}/lake/silver/scored_transactions}"
LABELS_CSV="${FRAUD_LABELS_CSV:-gs://${GCS_BUCKET}/inputs/transaction_log.csv}"
OUTPUT_BASE="${FRAUD_HOURLY_OUTPUT_BASE:-gs://${GCS_BUCKET}/lake/gold/hourly_batch}"
TARGET_HOUR_UTC="${FRAUD_TARGET_HOUR_UTC:-}"
EXTRA_SPARK_PROPERTIES="${GCP_DATAPROC_SPARK_PROPERTIES:-}"

script_args=(
  "--runtime-mode" "$RUNTIME_MODE"
  "--silver-path" "$SILVER_PATH"
  "--labels-csv" "$LABELS_CSV"
  "--output-base" "$OUTPUT_BASE"
  "--shuffle-partitions" "$SHUFFLE_PARTITIONS"
)

if [[ -n "$TARGET_HOUR_UTC" ]]; then
  script_args+=("--target-hour-utc" "$TARGET_HOUR_UTC")
fi

batch_flags=(
  --project="$PROJECT_ID"
  --region="$REGION"
  --deps-bucket="$DEPS_BUCKET"
)

# Dataproc batch ids must be unique per region; auto-suffix on collision for safe reruns.
if gcloud dataproc batches describe "$BATCH_ID" --project="$PROJECT_ID" --region="$REGION" >/dev/null 2>&1; then
  BATCH_ID="${BATCH_ID}-retry-$(date +%Y%m%d%H%M%S)"
fi

batch_flags+=("--batch=${BATCH_ID}")

if [[ -n "$EXTRA_SPARK_PROPERTIES" ]]; then
  batch_flags+=("--properties=${EXTRA_SPARK_PROPERTIES}")
fi

if [[ -n "${GCP_DATAPROC_SERVICE_ACCOUNT:-}" ]]; then
  batch_flags+=("--service-account=${GCP_DATAPROC_SERVICE_ACCOUNT}")
fi

if [[ -n "${GCP_DATAPROC_SUBNET:-}" ]]; then
  batch_flags+=("--subnet=${GCP_DATAPROC_SUBNET}")
fi

gcloud dataproc batches submit pyspark "$REPO_ROOT/batch/hourly_batch_processing.py" "${batch_flags[@]}" -- "${script_args[@]}"
