from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


PROJECT_ROOT = os.environ.get(
    "FRAUD_PROJECT_ROOT",
    str(Path(__file__).resolve().parents[2]),
)


def build_bash(command: str) -> str:
    return "\n".join(
        [
            "set -euo pipefail",
            f"cd \"{PROJECT_ROOT}\"",
            command,
        ]
    )


default_args = {
    "owner": "fraud-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


with DAG(
    dag_id="fraud_daily_model_refresh",
    description="Daily orchestration: refresh fraud model artifact from Silver + labels",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "ml", "model-refresh"],
) as dag:
    validate_runtime = BashOperator(
        task_id="validate_runtime",
        bash_command=build_bash(
            """
if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  echo "[daily-model] GOOGLE_APPLICATION_CREDENTIALS is required" >&2
  exit 1
fi

if [[ ! -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
  echo "[daily-model] Service account key not found: ${GOOGLE_APPLICATION_CREDENTIALS}" >&2
  exit 1
fi

if [[ -z "${FRAUD_GCP_PROJECT_ID:-}" ]]; then
    echo "[daily-model] FRAUD_GCP_PROJECT_ID is required" >&2
    exit 1
fi

command -v "${FRAUD_SPARK_SUBMIT_BIN:-spark-submit}" >/dev/null 2>&1 || {
  echo "[daily-model] spark-submit not found" >&2
  exit 1
}
"""
        ),
    )

    run_daily_model_refresh = BashOperator(
        task_id="run_daily_model_refresh",
        execution_timeout=timedelta(minutes=90),
        append_env=True,
        bash_command=build_bash(
            """
"${FRAUD_SPARK_SUBMIT_BIN:-spark-submit}" \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  batch/daily_model_refresh.py \
    --training-source bigquery \
    --project-id "${FRAUD_GCP_PROJECT_ID}" \
    --dataset "${FRAUD_BIGQUERY_DATASET:-fraud_analytics}" \
    --retraining-table "${FRAUD_BQ_RETRAINING_TABLE:-retraining_dataset}" \
  --model-output "${FRAUD_MODEL_OUTPUT}"
"""
        ),
        env={
                        "FRAUD_GCP_PROJECT_ID": os.environ.get("FRAUD_GCP_PROJECT_ID", ""),
                        "FRAUD_BIGQUERY_DATASET": os.environ.get("FRAUD_BIGQUERY_DATASET", "fraud_analytics"),
                        "FRAUD_BQ_RETRAINING_TABLE": os.environ.get("FRAUD_BQ_RETRAINING_TABLE", "retraining_dataset"),
            "FRAUD_MODEL_OUTPUT": os.environ.get("FRAUD_MODEL_OUTPUT", "gs://REPLACE_GOLD_BUCKET/models/fraud_rf_pipeline"),
            "FRAUD_SPARK_SUBMIT_BIN": os.environ.get("FRAUD_SPARK_SUBMIT_BIN", "/home/airflow/.local/bin/spark-submit"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    validate_runtime >> run_daily_model_refresh
