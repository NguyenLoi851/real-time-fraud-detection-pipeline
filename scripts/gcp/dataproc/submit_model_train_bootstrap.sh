#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to your GCP project id}"
REGION="${GCP_REGION:-us-central1}"
BATCH_ID="${GCP_DATAPROC_MODEL_BOOTSTRAP_BATCH_ID:-fraud-model-bootstrap-$(date +%Y%m%d%H%M%S)}"
GCS_BUCKET="${GCP_GCS_BUCKET:?Set GCP_GCS_BUCKET to the bucket that stores model artifacts}"
DEPS_BUCKET="${GCP_DATAPROC_DEPS_BUCKET:-$GCS_BUCKET}"
MODEL_OUTPUT="${FRAUD_MODEL_PATH:-gs://${GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline}"
TRAINING_INPUT_GCS="${FRAUD_BOOTSTRAP_TRAINING_INPUT_GCS:-gs://${GCS_BUCKET}/inputs/transaction_log.csv}"
LOCAL_TRAINING_INPUT="${FRAUD_BOOTSTRAP_TRAINING_INPUT_LOCAL:-$REPO_ROOT/data/transaction_log.csv}"
SHUFFLE_PARTITIONS="${FRAUD_SHUFFLE_PARTITIONS:-8}"
EXTRA_SPARK_PROPERTIES="${GCP_DATAPROC_SPARK_PROPERTIES:-}"

if ! gsutil ls "$TRAINING_INPUT_GCS" >/dev/null 2>&1; then
  if [[ -f "$LOCAL_TRAINING_INPUT" ]]; then
    gsutil cp "$LOCAL_TRAINING_INPUT" "$TRAINING_INPUT_GCS"
  else
    echo "ERROR: Training input missing in GCS and local fallback not found."
    echo "Missing GCS object: $TRAINING_INPUT_GCS"
    echo "Missing local file: $LOCAL_TRAINING_INPUT"
    exit 1
  fi
fi

script_args=(
  "--input" "$TRAINING_INPUT_GCS"
  "--model-output" "$MODEL_OUTPUT"
  "--shuffle-partitions" "$SHUFFLE_PARTITIONS"
)

batch_flags=(
  --project="$PROJECT_ID"
  --region="$REGION"
  --deps-bucket="$DEPS_BUCKET"
)

if [[ -n "$EXTRA_SPARK_PROPERTIES" ]]; then
  batch_flags+=("--properties=${EXTRA_SPARK_PROPERTIES}")
fi

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

gcloud dataproc batches submit pyspark "$REPO_ROOT/ml/train_fraud_model.py" "${batch_flags[@]}" -- "${script_args[@]}"
