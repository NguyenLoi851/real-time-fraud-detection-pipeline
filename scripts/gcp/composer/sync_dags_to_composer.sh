#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSER_ENV_NAME="${COMPOSER_ENV_NAME:?Set COMPOSER_ENV_NAME to your Composer environment name}"
COMPOSER_REGION="${COMPOSER_REGION:-us-central1}"
COMPOSER_PROJECT_ID="${COMPOSER_PROJECT_ID:-${GCP_PROJECT_ID:-}}"

if [[ -z "${COMPOSER_PROJECT_ID}" ]]; then
  echo "[composer-sync] Set COMPOSER_PROJECT_ID (or GCP_PROJECT_ID) before running." >&2
  exit 1
fi

airflow_dags_path="$(gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
  --location "${COMPOSER_REGION}" \
  --project "${COMPOSER_PROJECT_ID}" \
  --format='value(config.dagGcsPrefix)')"

if [[ -z "${airflow_dags_path}" ]]; then
  echo "[composer-sync] Failed to resolve DAG bucket path from Composer environment." >&2
  exit 1
fi

echo "[composer-sync] Syncing DAG files to ${airflow_dags_path}"
gsutil -m cp \
  "${REPO_ROOT}/airflow/dags/fraud_hourly_orchestration.py" \
  "${REPO_ROOT}/airflow/dags/fraud_daily_model_refresh.py" \
  "${airflow_dags_path}/"

echo "[composer-sync] DAG sync completed."
