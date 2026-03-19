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
ALERT_EMAILS = [
    email.strip()
    for email in os.environ.get("FRAUD_ALERT_EMAILS", "").split(",")
    if email.strip()
]


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
    "retry_delay": timedelta(minutes=10),
    "email": ALERT_EMAILS,
    "email_on_retry": bool(ALERT_EMAILS),
    "email_on_failure": bool(ALERT_EMAILS),
}


with DAG(
    dag_id="fraud_hourly_batch_and_warehouse",
    description="Hourly orchestration: Spark batch -> BigQuery load -> dbt run/test",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "batch", "dbt", "bigquery"],
) as dag:
    validate_runtime = BashOperator(
        task_id="validate_runtime",
        bash_command=build_bash(
            """
if [[ -z \"${FRAUD_GCP_PROJECT_ID:-}\" ]]; then
  echo \"[step8] FRAUD_GCP_PROJECT_ID is required\" >&2
  exit 1
fi

if [[ -z \"${GOOGLE_APPLICATION_CREDENTIALS:-}\" ]]; then
  echo \"[step8] GOOGLE_APPLICATION_CREDENTIALS is required\" >&2
  exit 1
fi

if [[ ! -f \"${GOOGLE_APPLICATION_CREDENTIALS}\" ]]; then
  echo \"[step8] Service account key not found: ${GOOGLE_APPLICATION_CREDENTIALS}\" >&2
  exit 1
fi

command -v "${FRAUD_SPARK_SUBMIT_BIN:-spark-submit}" >/dev/null 2>&1 || {
  echo \"[step8] spark-submit not found\" >&2
  exit 1
}

command -v "${FRAUD_PYTHON_BIN:-python3}" >/dev/null 2>&1 || {
  echo \"[step8] python3 not found\" >&2
  exit 1
}

command -v dbt >/dev/null 2>&1 || {
  echo \"[step8] dbt not found (install dbt and ensure it is on PATH)\" >&2
  exit 1
}
"""
        ),
    )

    run_hourly_batch = BashOperator(
        task_id="run_hourly_batch",
        execution_timeout=timedelta(minutes=60),
        append_env=True,
        bash_command=build_bash(
            """
"${FRAUD_SPARK_SUBMIT_BIN:-spark-submit}" \
    --driver-memory "${FRAUD_SPARK_DRIVER_MEMORY:-4g}" \
    --executor-memory "${FRAUD_SPARK_EXECUTOR_MEMORY:-4g}" \
    --conf "spark.driver.maxResultSize=${FRAUD_SPARK_DRIVER_MAX_RESULT_SIZE:-1g}" \
    --conf "spark.sql.shuffle.partitions=${FRAUD_HOURLY_BATCH_SHUFFLE_PARTITIONS:-8}" \
  --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
  batch/hourly_batch_processing.py \
  --silver-path "${FRAUD_SILVER_PATH:-data/lake/silver/scored_transactions}" \
  --labels-csv "${FRAUD_LABELS_CSV:-data/transaction_log.csv}" \
  --output-base "${FRAUD_BATCH_OUTPUT_BASE}" \
    --shuffle-partitions "${FRAUD_HOURLY_BATCH_SHUFFLE_PARTITIONS:-8}" \
"""
        ),
        env={
            "FRAUD_BATCH_OUTPUT_BASE": os.environ.get("FRAUD_BATCH_OUTPUT_BASE", "gs://REPLACE_GOLD_BUCKET/hourly_batch"),
            "FRAUD_SPARK_SUBMIT_BIN": os.environ.get("FRAUD_SPARK_SUBMIT_BIN", "/home/airflow/.local/bin/spark-submit"),
            "FRAUD_SPARK_DRIVER_MEMORY": os.environ.get("FRAUD_SPARK_DRIVER_MEMORY", "4g"),
            "FRAUD_SPARK_EXECUTOR_MEMORY": os.environ.get("FRAUD_SPARK_EXECUTOR_MEMORY", "4g"),
            "FRAUD_SPARK_DRIVER_MAX_RESULT_SIZE": os.environ.get("FRAUD_SPARK_DRIVER_MAX_RESULT_SIZE", "1g"),
            "FRAUD_HOURLY_BATCH_SHUFFLE_PARTITIONS": os.environ.get("FRAUD_HOURLY_BATCH_SHUFFLE_PARTITIONS", "8"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    load_batch_to_bigquery = BashOperator(
        task_id="load_batch_to_bigquery",
        execution_timeout=timedelta(minutes=25),
        append_env=True,
        bash_command=build_bash(
            """
"${FRAUD_PYTHON_BIN:-python3}" batch/load_hourly_batch_to_bigquery.py \
  --project-id "${FRAUD_GCP_PROJECT_ID}" \
  --dataset "${FRAUD_BIGQUERY_DATASET:-fraud_analytics}" \
  --gcs-output-base "${FRAUD_BATCH_OUTPUT_BASE}" \
  --write-disposition WRITE_TRUNCATE \
  --create-dataset-if-missing
"""
        ),
        env={
            "FRAUD_GCP_PROJECT_ID": os.environ.get("FRAUD_GCP_PROJECT_ID", ""),
            "FRAUD_BIGQUERY_DATASET": os.environ.get("FRAUD_BIGQUERY_DATASET", "fraud_analytics"),
            "FRAUD_BATCH_OUTPUT_BASE": os.environ.get("FRAUD_BATCH_OUTPUT_BASE", "gs://REPLACE_GOLD_BUCKET/hourly_batch"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        execution_timeout=timedelta(minutes=20),
        append_env=True,
        bash_command=build_bash(
            """
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${FRAUD_PROJECT_ROOT:-$PWD}/dbt}"
export DBT_BIGQUERY_PROJECT="${DBT_BIGQUERY_PROJECT:-${FRAUD_GCP_PROJECT_ID}}"
export DBT_BIGQUERY_DATASET="${DBT_BIGQUERY_DATASET:-${FRAUD_BIGQUERY_DATASET:-fraud_analytics}}"

cd dbt
dbt deps
dbt run
"""
        ),
        env={
            "FRAUD_PROJECT_ROOT": PROJECT_ROOT,
            "FRAUD_GCP_PROJECT_ID": os.environ.get("FRAUD_GCP_PROJECT_ID", ""),
            "FRAUD_BIGQUERY_DATASET": os.environ.get("FRAUD_BIGQUERY_DATASET", "fraud_analytics"),
            "DBT_PROFILES_DIR": os.environ.get("DBT_PROFILES_DIR", str(Path(PROJECT_ROOT) / "dbt")),
            "DBT_BIGQUERY_PROJECT": os.environ.get("DBT_BIGQUERY_PROJECT", ""),
            "DBT_BIGQUERY_DATASET": os.environ.get("DBT_BIGQUERY_DATASET", "fraud_analytics"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        execution_timeout=timedelta(minutes=15),
        append_env=True,
        bash_command=build_bash(
            """
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-${FRAUD_PROJECT_ROOT:-$PWD}/dbt}"
export DBT_BIGQUERY_PROJECT="${DBT_BIGQUERY_PROJECT:-${FRAUD_GCP_PROJECT_ID}}"
export DBT_BIGQUERY_DATASET="${DBT_BIGQUERY_DATASET:-${FRAUD_BIGQUERY_DATASET:-fraud_analytics}}"

cd dbt
dbt test
"""
        ),
        env={
            "FRAUD_PROJECT_ROOT": PROJECT_ROOT,
            "FRAUD_GCP_PROJECT_ID": os.environ.get("FRAUD_GCP_PROJECT_ID", ""),
            "FRAUD_BIGQUERY_DATASET": os.environ.get("FRAUD_BIGQUERY_DATASET", "fraud_analytics"),
            "DBT_PROFILES_DIR": os.environ.get("DBT_PROFILES_DIR", str(Path(PROJECT_ROOT) / "dbt")),
            "DBT_BIGQUERY_PROJECT": os.environ.get("DBT_BIGQUERY_PROJECT", ""),
            "DBT_BIGQUERY_DATASET": os.environ.get("DBT_BIGQUERY_DATASET", "fraud_analytics"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    validate_runtime >> run_hourly_batch >> load_batch_to_bigquery >> run_dbt_models >> run_dbt_tests
