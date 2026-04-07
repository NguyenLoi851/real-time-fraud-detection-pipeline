#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROJECT_ID="${GCP_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "(unset)" ]]; then
  cat >&2 <<'EOF'
ERROR: Missing GCP project id.

Set one of the following before running this script:
  export GCP_PROJECT_ID="<your-project-id>"
or configure gcloud default project:
  gcloud config set project "<your-project-id>"
EOF
  exit 1
fi

if [[ -z "${GCP_GCS_BUCKET:-}" ]]; then
  cat >&2 <<'EOF'
ERROR: Missing GCS bucket for model artifacts.

Set this before running:
  export GCP_GCS_BUCKET="<your-lake-and-artifacts-bucket>"

Optional related values:
  export GCP_REGION="us-central1"
  export GCP_DATAPROC_DEPS_BUCKET="${GCP_GCS_BUCKET}"
  export PIPELINE_MODE="gcp-native"

Then re-run:
  bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
EOF
  exit 1
fi

REGION="${GCP_REGION:-us-central1}"
BATCH_ID="${GCP_DATAPROC_DAILY_BATCH_ID:-fraud-daily-model-refresh-$(date +%Y%m%d%H%M%S)}"
RUNTIME_MODE="${PIPELINE_MODE:-gcp-native}"
GCS_BUCKET="${GCP_GCS_BUCKET}"
DEPS_BUCKET="${GCP_DATAPROC_DEPS_BUCKET:-$GCS_BUCKET}"
RUNTIME_SERVICE_ACCOUNT="${GCP_DATAPROC_SERVICE_ACCOUNT:-dataproc-fraud-runtime@${PROJECT_ID}.iam.gserviceaccount.com}"
MODEL_OUTPUT="${FRAUD_MODEL_OUTPUT:-gs://${GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline}"
TRAINING_SOURCE="${FRAUD_TRAINING_SOURCE:-bigquery}"
DATASET="${FRAUD_BQ_DATASET:-fraud_analytics}"
RETRAINING_TABLE="${FRAUD_RETRAINING_TABLE:-retraining_dataset}"
MODEL_SEED="${FRAUD_MODEL_SEED:-42}"
MODEL_NUM_TREES="${FRAUD_MODEL_NUM_TREES:-120}"
MODEL_MAX_DEPTH="${FRAUD_MODEL_MAX_DEPTH:-8}"
BIGQUERY_CONNECTOR_PACKAGE="${BIGQUERY_CONNECTOR_PACKAGE:-}"
USE_BIGQUERY_CONNECTOR_OVERRIDE="${GCP_DATAPROC_USE_BIGQUERY_CONNECTOR_OVERRIDE:-false}"
EXTRA_SPARK_PROPERTIES="${GCP_DATAPROC_SPARK_PROPERTIES:-}"

SPARK_PROPERTIES=""
if [[ "$TRAINING_SOURCE" == "bigquery" && "$USE_BIGQUERY_CONNECTOR_OVERRIDE" == "true" && -n "$BIGQUERY_CONNECTOR_PACKAGE" ]]; then
  SPARK_PROPERTIES="spark.jars.packages=${BIGQUERY_CONNECTOR_PACKAGE}"
elif [[ "$TRAINING_SOURCE" == "bigquery" && -n "$BIGQUERY_CONNECTOR_PACKAGE" && "$USE_BIGQUERY_CONNECTOR_OVERRIDE" != "true" ]]; then
  cat >&2 <<'EOF'
INFO: Ignoring BIGQUERY_CONNECTOR_PACKAGE because GCP_DATAPROC_USE_BIGQUERY_CONNECTOR_OVERRIDE is not true.
Using Dataproc's built-in BigQuery connector to avoid classpath conflicts.
EOF
fi
if [[ -n "$EXTRA_SPARK_PROPERTIES" ]]; then
  SPARK_PROPERTIES+="${SPARK_PROPERTIES:+,}${EXTRA_SPARK_PROPERTIES}"
fi

script_args=(
  "--runtime-mode" "$RUNTIME_MODE"
  "--training-source" "$TRAINING_SOURCE"
  "--project-id" "$PROJECT_ID"
  "--dataset" "$DATASET"
  "--retraining-table" "$RETRAINING_TABLE"
  "--model-output" "$MODEL_OUTPUT"
  "--model-seed" "$MODEL_SEED"
  "--model-num-trees" "$MODEL_NUM_TREES"
  "--model-max-depth" "$MODEL_MAX_DEPTH"
)

batch_flags=(
  --project="$PROJECT_ID"
  --region="$REGION"
  --deps-bucket="$DEPS_BUCKET"
)

if [[ -n "$SPARK_PROPERTIES" ]]; then
  batch_flags+=("--properties=${SPARK_PROPERTIES}")
fi

# Dataproc batch ids must be unique per region; auto-suffix on collision for safe reruns.
if gcloud dataproc batches describe "$BATCH_ID" --project="$PROJECT_ID" --region="$REGION" >/dev/null 2>&1; then
  BATCH_ID="${BATCH_ID}-retry-$(date +%Y%m%d%H%M%S)"
fi

batch_flags+=("--batch=${BATCH_ID}")

if [[ -n "$RUNTIME_SERVICE_ACCOUNT" ]]; then
  batch_flags+=("--service-account=${RUNTIME_SERVICE_ACCOUNT}")
fi

if [[ -n "${GCP_DATAPROC_SUBNET:-}" ]]; then
  batch_flags+=("--subnet=${GCP_DATAPROC_SUBNET}")
fi

gcloud dataproc batches submit pyspark "$REPO_ROOT/batch/daily_model_refresh.py" "${batch_flags[@]}" -- "${script_args[@]}"
