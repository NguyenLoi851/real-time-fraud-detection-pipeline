from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor
from airflow.utils.dates import days_ago
from google.cloud.dataform_v1beta1.types import WorkflowInvocation


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
DATAFORM_REGION = cfg("DATAFORM_REGION", REGION)
DATAFORM_REPOSITORY_ID = cfg("DATAFORM_REPOSITORY_ID", "fraud-warehouse")
DATAFORM_RUN_WORKFLOW_CONFIG_ID = cfg("DATAFORM_RUN_WORKFLOW_CONFIG_ID", "fraud-main")
DATAFORM_ASSERT_WORKFLOW_CONFIG_ID = cfg("DATAFORM_ASSERT_WORKFLOW_CONFIG_ID", "fraud-assertions")

HOURLY_BATCH_PY_URI = cfg(
    "FRAUD_HOURLY_BATCH_PY_URI",
    f"gs://{GCS_BUCKET}/code/batch/hourly_batch_processing.py" if GCS_BUCKET else "",
)
HOURLY_BQ_LOAD_PY_URI = cfg(
    "FRAUD_HOURLY_BQ_LOAD_PY_URI",
    f"gs://{GCS_BUCKET}/code/batch/load_hourly_batch_to_bigquery.py" if GCS_BUCKET else "",
)

SILVER_PATH = cfg("FRAUD_SILVER_PATH", f"gs://{GCS_BUCKET}/lake/silver/scored_transactions" if GCS_BUCKET else "")
LABELS_CSV = cfg("FRAUD_LABELS_CSV", f"gs://{GCS_BUCKET}/inputs/transaction_log.csv" if GCS_BUCKET else "")
OUTPUT_BASE = cfg("FRAUD_HOURLY_OUTPUT_BASE", f"gs://{GCS_BUCKET}/lake/gold/hourly_batch" if GCS_BUCKET else "")
BQ_DATASET = cfg("FRAUD_BQ_DATASET", cfg("FRAUD_BIGQUERY_DATASET", "fraud_analytics"))
BQ_TEMP_BUCKET = cfg("FRAUD_BQ_TEMP_BUCKET", GCS_BUCKET)
HOURLY_SHUFFLE_PARTITIONS = cfg("FRAUD_SHUFFLE_PARTITIONS", "8")

ALERT_EMAILS = [
    email.strip()
    for email in os.environ.get("FRAUD_ALERT_EMAILS", "").split(",")
    if email.strip()
]


def validate_required_settings() -> None:
    required = {
        "FRAUD_GCP_PROJECT_ID": PROJECT_ID,
        "GCP_GCS_BUCKET": GCS_BUCKET,
        "FRAUD_HOURLY_BATCH_PY_URI": HOURLY_BATCH_PY_URI,
        "FRAUD_HOURLY_BQ_LOAD_PY_URI": HOURLY_BQ_LOAD_PY_URI,
        "DATAFORM_REPOSITORY_ID": DATAFORM_REPOSITORY_ID,
        "DATAFORM_RUN_WORKFLOW_CONFIG_ID": DATAFORM_RUN_WORKFLOW_CONFIG_ID,
        "DATAFORM_ASSERT_WORKFLOW_CONFIG_ID": DATAFORM_ASSERT_WORKFLOW_CONFIG_ID,
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
    "retry_delay": timedelta(minutes=10),
    "email": ALERT_EMAILS,
    "email_on_retry": bool(ALERT_EMAILS),
    "email_on_failure": bool(ALERT_EMAILS),
}


with DAG(
    dag_id="fraud_hourly_batch_and_warehouse",
    description="Hourly orchestration: Dataproc batch -> BigQuery load -> Dataform run/assert",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "batch", "composer", "dataform", "bigquery"],
) as dag:
    validate_runtime = PythonOperator(
        task_id="validate_runtime",
    python_callable=validate_required_settings,
    )

    run_hourly_batch = DataprocCreateBatchOperator(
        task_id="run_hourly_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="fraud-hourly-batch-{{ ts_nodash | lower }}",
        batch=build_dataproc_batch_config(
            HOURLY_BATCH_PY_URI,
            [
                "--runtime-mode",
                "gcp-native",
                "--silver-path",
                SILVER_PATH,
                "--labels-csv",
                LABELS_CSV,
                "--output-base",
                OUTPUT_BASE,
                "--shuffle-partitions",
                HOURLY_SHUFFLE_PARTITIONS,
            ],
        ),
        gcp_conn_id="google_cloud_default",
        execution_timeout=timedelta(minutes=75),
        deferrable=True,
    )

    wait_hourly_batch = DataprocBatchSensor(
        task_id="wait_hourly_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="fraud-hourly-batch-{{ ts_nodash | lower }}",
        gcp_conn_id="google_cloud_default",
        timeout=60 * 75,
        poke_interval=60,
    )

    load_batch_to_bigquery = DataprocCreateBatchOperator(
        task_id="load_batch_to_bigquery",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="fraud-hourly-bq-load-{{ ts_nodash | lower }}",
        batch=build_dataproc_batch_config(
            HOURLY_BQ_LOAD_PY_URI,
            [
                "--runtime-mode",
                "gcp-native",
                "--project-id",
                PROJECT_ID,
                "--dataset",
                BQ_DATASET,
                "--gcs-output-base",
                OUTPUT_BASE,
                "--write-disposition",
                "WRITE_TRUNCATE",
                "--create-dataset-if-missing",
                "--temporary-gcs-bucket",
                BQ_TEMP_BUCKET,
            ],
        ),
        gcp_conn_id="google_cloud_default",
        execution_timeout=timedelta(minutes=35),
        deferrable=True,
    )

    wait_bq_load = DataprocBatchSensor(
        task_id="wait_bq_load",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="fraud-hourly-bq-load-{{ ts_nodash | lower }}",
        gcp_conn_id="google_cloud_default",
        timeout=60 * 35,
        poke_interval=60,
    )

    run_dataform_models = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform_models",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation={
            "workflow_config": (
                f"projects/{PROJECT_ID}/locations/{DATAFORM_REGION}/repositories/"
                f"{DATAFORM_REPOSITORY_ID}/workflowConfigs/{DATAFORM_RUN_WORKFLOW_CONFIG_ID}"
            )
        },
        gcp_conn_id="google_cloud_default",
    )

    wait_dataform_models = DataformWorkflowInvocationStateSensor(
        task_id="wait_dataform_models",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ ti.xcom_pull(task_ids='run_dataform_models')['name'].split('/')[-1] }}"
        ),
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
        failure_statuses={
            WorkflowInvocation.State.FAILED,
            WorkflowInvocation.State.CANCELLED,
            WorkflowInvocation.State.CANCELING,
        },
        gcp_conn_id="google_cloud_default",
        timeout=60 * 30,
        poke_interval=30,
    )

    run_dataform_assertions = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform_assertions",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation={
            "workflow_config": (
                f"projects/{PROJECT_ID}/locations/{DATAFORM_REGION}/repositories/"
                f"{DATAFORM_REPOSITORY_ID}/workflowConfigs/{DATAFORM_ASSERT_WORKFLOW_CONFIG_ID}"
            )
        },
        gcp_conn_id="google_cloud_default",
    )

    wait_dataform_assertions = DataformWorkflowInvocationStateSensor(
        task_id="wait_dataform_assertions",
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ ti.xcom_pull(task_ids='run_dataform_assertions')['name'].split('/')[-1] }}"
        ),
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
        failure_statuses={
            WorkflowInvocation.State.FAILED,
            WorkflowInvocation.State.CANCELLED,
            WorkflowInvocation.State.CANCELING,
        },
        gcp_conn_id="google_cloud_default",
        timeout=60 * 30,
        poke_interval=30,
    )

    (
        validate_runtime
        >> run_hourly_batch
        >> wait_hourly_batch
        >> load_batch_to_bigquery
        >> wait_bq_load
        >> run_dataform_models
        >> wait_dataform_models
        >> run_dataform_assertions
        >> wait_dataform_assertions
    )
