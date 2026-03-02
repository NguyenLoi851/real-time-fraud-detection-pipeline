# Orchestration with Airflow (Roadmap Step 8)

This module runs Airflow in Docker and automates the hourly batch + warehouse flow:

1. Spark hourly batch (`batch/hourly_batch_processing.py`)
2. BigQuery load from GCS (`batch/load_hourly_batch_to_bigquery.py`)
3. dbt models + tests (`dbt run`, `dbt test`)

DAG file:

- `airflow/dags/fraud_hourly_orchestration.py`

## 1) Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin) is installed and running.
- GCP service account key file exists at `infra/terraform/keys/terraform-sa-key.json`.

## 2) Configure environment for containers

Copy and edit the environment file:

```bash
cp airflow/.env.example airflow/.env
```

Update at least:

- `FRAUD_GCP_PROJECT_ID`
- `DBT_BIGQUERY_PROJECT`
- `FRAUD_SILVER_PATH`
- `FRAUD_BATCH_OUTPUT_BASE`
- `FRAUD_MODEL_OUTPUT`

## 3) Start Airflow with Docker Compose

Option A (helper scripts):

```bash
chmod +x scripts/airflow/airflow_up.sh scripts/airflow/airflow_down.sh
./scripts/airflow/airflow_up.sh
```

Option B (direct compose):

```bash
cd airflow
docker compose -f docker-compose.airflow.yml up -d --build
```

Airflow UI:

- URL: `http://localhost:8080`
- User: `admin`
- Password: `admin`

Stop stack:

```bash
./scripts/airflow/airflow_down.sh
```

## 4) What the DAG does

- `validate_runtime`: checks required env vars + binaries.
- `run_hourly_batch`: processes one hour slice using Airflow `data_interval_start` in UTC.
- `load_batch_to_bigquery`: loads parquet outputs from `FRAUD_BATCH_OUTPUT_BASE` to BigQuery.
- `run_dbt_models`: rebuilds warehouse models.
- `run_dbt_tests`: runs dbt tests.

## 5) Notes

- The custom image is defined in `airflow/Dockerfile` and includes Java, PySpark, BigQuery client, and dbt-bigquery.
- The project root is mounted into the container at `/opt/project`.
- Ensure the service account has read/write access to GCS and BigQuery dataset permissions.
- `run_hourly_batch` includes periodic model refresh using latest labeled records.
- If you see `spark-submit: command not found`, set `FRAUD_SPARK_SUBMIT_BIN=/home/airflow/.local/bin/spark-submit` in `airflow/.env` and restart Airflow services.
