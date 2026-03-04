#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AIRFLOW_ENV_FILE="$ROOT_DIR/airflow/.env"

required_vars=(
  FRAUD_GCP_PROJECT_ID
  FRAUD_SILVER_BUCKET
  FRAUD_GOLD_BUCKET
  FRAUD_BQ_DATASET
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "[airflow-env] Missing required environment variable: ${var_name}" >&2
    echo "[airflow-env] Export step 6.1 variables first, then rerun this script." >&2
    exit 1
  fi
done

cat > "$AIRFLOW_ENV_FILE" <<EOF
FRAUD_PROJECT_ROOT=/opt/project
GOOGLE_APPLICATION_CREDENTIALS=/opt/project/infra/terraform/keys/terraform-sa-key.json

FRAUD_GCP_PROJECT_ID=${FRAUD_GCP_PROJECT_ID}
FRAUD_BIGQUERY_DATASET=${FRAUD_BQ_DATASET}
FRAUD_BQ_RETRAINING_TABLE=retraining_dataset

FRAUD_SILVER_PATH=gs://${FRAUD_SILVER_BUCKET}/scored_transactions
FRAUD_BATCH_OUTPUT_BASE=gs://${FRAUD_GOLD_BUCKET}/hourly_batch
FRAUD_MODEL_OUTPUT=gs://${FRAUD_GOLD_BUCKET}/models/fraud_rf_pipeline
FRAUD_LABELS_CSV=/opt/project/data/transaction_log.csv

DBT_PROFILES_DIR=/opt/project/dbt
DBT_BIGQUERY_PROJECT=${FRAUD_GCP_PROJECT_ID}
DBT_BIGQUERY_DATASET=${FRAUD_BQ_DATASET}

FRAUD_SPARK_SUBMIT_BIN=/home/airflow/.local/bin/spark-submit
FRAUD_PYTHON_BIN=python3
EOF

echo "[airflow-env] Wrote $AIRFLOW_ENV_FILE"
echo "[airflow-env] Preview:"
grep -E 'FRAUD_GCP_PROJECT_ID|FRAUD_SILVER_PATH|FRAUD_BATCH_OUTPUT_BASE|FRAUD_MODEL_OUTPUT|FRAUD_BIGQUERY_DATASET|FRAUD_BQ_RETRAINING_TABLE' "$AIRFLOW_ENV_FILE"
