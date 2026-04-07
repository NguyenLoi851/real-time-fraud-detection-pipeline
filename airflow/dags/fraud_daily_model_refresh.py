from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.utils.dates import days_ago


def cfg(name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.environ.get(name, default))


PROJECT_ID = cfg("FRAUD_GCP_PROJECT_ID")
REGION = cfg("GCP_REGION", "us-central1")
GCS_BUCKET = cfg("GCP_GCS_BUCKET", "")
SERVICE_ACCOUNT = cfg("GCP_DATAPROC_SERVICE_ACCOUNT", "")
SUBNET_URI = cfg("GCP_DATAPROC_SUBNET", "")
SPARK_PROPERTIES = cfg(
    "GCP_DATAPROC_SPARK_PROPERTIES",
    "spark.dynamicAllocation.enabled=false,spark.driver.cores=4,spark.executor.instances=2,spark.executor.cores=4",
)

DAILY_REFRESH_PY_URI = cfg(
    "FRAUD_DAILY_MODEL_REFRESH_PY_URI",
    f"gs://{GCS_BUCKET}/code/batch/daily_model_refresh.py" if GCS_BUCKET else "",
)
MODEL_OUTPUT = cfg("FRAUD_MODEL_OUTPUT", f"gs://{GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline" if GCS_BUCKET else "")
BQ_DATASET = cfg("FRAUD_BQ_DATASET", cfg("FRAUD_BIGQUERY_DATASET", "fraud_analytics"))
BQ_RETRAINING_TABLE = cfg("FRAUD_RETRAINING_TABLE", cfg("FRAUD_BQ_RETRAINING_TABLE", "retraining_dataset"))

ALERT_EMAILS = [
    email.strip()
    for email in os.environ.get("FRAUD_ALERT_EMAILS", "").split(",")
    if email.strip()
]


def validate_required_settings() -> None:
    required = {
        "FRAUD_GCP_PROJECT_ID": PROJECT_ID,
        "GCP_GCS_BUCKET": GCS_BUCKET,
        "FRAUD_DAILY_MODEL_REFRESH_PY_URI": DAILY_REFRESH_PY_URI,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError(f"Missing required Composer variables: {', '.join(missing)}")


def build_dataproc_batch_config(main_python_file_uri: str, args: list[str]) -> dict:
    config: dict = {
        "pyspark_batch": {
            "main_python_file_uri": main_python_file_uri,
            "args": args,
        },
        "runtime_config": {
            "properties": {
                item.split("=", 1)[0]: item.split("=", 1)[1]
                for item in SPARK_PROPERTIES.split(",")
                if "=" in item and item.strip()
            }
        },
    }

    execution_config = {}
    if SERVICE_ACCOUNT:
        execution_config["service_account"] = SERVICE_ACCOUNT
    if SUBNET_URI:
        execution_config["subnetwork_uri"] = SUBNET_URI
    if execution_config:
        config["environment_config"] = {"execution_config": execution_config}

    return config


default_args = {
    "owner": "fraud-pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email": ALERT_EMAILS,
    "email_on_retry": bool(ALERT_EMAILS),
    "email_on_failure": bool(ALERT_EMAILS),
}


with DAG(
    dag_id="fraud_daily_model_refresh",
    description="Daily orchestration: Dataproc model refresh using BigQuery retraining table",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "ml", "model-refresh", "composer"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_runtime",
        python_callable=validate_required_settings,
    )

    run_daily_model_refresh = DataprocCreateBatchOperator(
        task_id="run_daily_model_refresh",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="fraud-daily-model-refresh-{{ ts_nodash | lower }}",
        batch=build_dataproc_batch_config(
            DAILY_REFRESH_PY_URI,
            [
                "--runtime-mode",
                "gcp-native",
                "--training-source",
                "bigquery",
                "--project-id",
                PROJECT_ID,
                "--dataset",
                BQ_DATASET,
                "--retraining-table",
                BQ_RETRAINING_TABLE,
                "--model-output",
                MODEL_OUTPUT,
            ],
        ),
        gcp_conn_id="google_cloud_default",
        execution_timeout=timedelta(minutes=90),
        deferrable=True,
    )

    wait_daily_model_refresh = DataprocBatchSensor(
        task_id="wait_daily_model_refresh",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="fraud-daily-model-refresh-{{ ts_nodash | lower }}",
        gcp_conn_id="google_cloud_default",
        timeout=60 * 90,
        poke_interval=60,
    )

    validate_runtime >> run_daily_model_refresh >> wait_daily_model_refresh
