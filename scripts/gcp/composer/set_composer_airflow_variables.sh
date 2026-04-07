#!/usr/bin/env bash
set -euo pipefail

COMPOSER_ENV_NAME="${COMPOSER_ENV_NAME:?Set COMPOSER_ENV_NAME to your Composer environment name}"
COMPOSER_REGION="${COMPOSER_REGION:-us-central1}"
COMPOSER_PROJECT_ID="${COMPOSER_PROJECT_ID:-${GCP_PROJECT_ID:-}}"

if [[ -z "${COMPOSER_PROJECT_ID}" ]]; then
  echo "[composer-vars] Set COMPOSER_PROJECT_ID (or GCP_PROJECT_ID) before running." >&2
  exit 1
fi

required_vars=(
  FRAUD_GCP_PROJECT_ID
  GCP_GCS_BUCKET
  DATAFORM_REPOSITORY_ID
  DATAFORM_RUN_WORKFLOW_CONFIG_ID
  DATAFORM_ASSERT_WORKFLOW_CONFIG_ID
  FRAUD_HOURLY_BATCH_PY_URI
  FRAUD_HOURLY_BQ_LOAD_PY_URI
  FRAUD_DAILY_MODEL_REFRESH_PY_URI
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "[composer-vars] Missing required environment variable: ${var_name}" >&2
    exit 1
  fi
done

vars_to_set=(
  FRAUD_GCP_PROJECT_ID
  GCP_REGION
  GCP_GCS_BUCKET
  GCP_DATAPROC_DEPS_BUCKET
  GCP_DATAPROC_SERVICE_ACCOUNT
  GCP_DATAPROC_SUBNET
  GCP_DATAPROC_SPARK_PROPERTIES
  FRAUD_SILVER_PATH
  FRAUD_LABELS_CSV
  FRAUD_HOURLY_OUTPUT_BASE
  FRAUD_BQ_DATASET
  FRAUD_BQ_TEMP_BUCKET
  FRAUD_RETRAINING_TABLE
  FRAUD_MODEL_OUTPUT
  DATAFORM_REGION
  DATAFORM_REPOSITORY_ID
  DATAFORM_RUN_WORKFLOW_CONFIG_ID
  DATAFORM_ASSERT_WORKFLOW_CONFIG_ID
  FRAUD_HOURLY_BATCH_PY_URI
  FRAUD_HOURLY_BQ_LOAD_PY_URI
  FRAUD_DAILY_MODEL_REFRESH_PY_URI
)

for var_name in "${vars_to_set[@]}"; do
  var_value="${!var_name:-}"
  if [[ -n "${var_value}" ]]; then
    echo "[composer-vars] Setting ${var_name}"
    gcloud composer environments run "${COMPOSER_ENV_NAME}" \
      --location "${COMPOSER_REGION}" \
      --project "${COMPOSER_PROJECT_ID}" \
      variables set -- "${var_name}" "${var_value}"
  fi
done

echo "[composer-vars] Variable sync completed."
