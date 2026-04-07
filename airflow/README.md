# Airflow Orchestration

## Purpose

Run orchestration DAGs that support the cloud-native Composer + Dataproc + Dataform runtime.

Pipeline DAGs:

- `airflow/dags/fraud_hourly_orchestration.py`
- `airflow/dags/fraud_daily_model_refresh.py`

## Inputs and Outputs

- Inputs: Silver scored transactions, labels history, Airflow Variables, Dataform workflow configs.
- Outputs: Curated parquet on GCS, BigQuery loaded tables, Dataform warehouse transforms/assertions, refreshed model artifact.

## Prerequisites

Complete shared setup first: [../docs/prerequisites.md](../docs/prerequisites.md)

## Configure Environment

```bash
cp airflow/.env.example airflow/.env
```

Or generate `airflow/.env` from exported runtime variables:

```bash
./scripts/airflow/write_airflow_env.sh
```

Update at least:

- `FRAUD_GCP_PROJECT_ID`
- `GCP_GCS_BUCKET`
- `FRAUD_BQ_DATASET`
- `FRAUD_RETRAINING_TABLE`
- `FRAUD_SILVER_PATH`
- `FRAUD_HOURLY_OUTPUT_BASE`
- `FRAUD_MODEL_OUTPUT`
- `FRAUD_HOURLY_BATCH_PY_URI`
- `FRAUD_HOURLY_BQ_LOAD_PY_URI`
- `FRAUD_DAILY_MODEL_REFRESH_PY_URI`
- `DATAFORM_REPOSITORY_ID`
- `DATAFORM_RUN_WORKFLOW_CONFIG_ID`
- `DATAFORM_ASSERT_WORKFLOW_CONFIG_ID`

Optional for task alerts:

- `FRAUD_ALERT_EMAILS` (comma-separated recipients, e.g. `alerts@company.com,oncall@company.com`)
- SMTP transport in `airflow/.env`:
	- `AIRFLOW__SMTP__SMTP_HOST`
	- `AIRFLOW__SMTP__SMTP_PORT`
	- `AIRFLOW__SMTP__SMTP_MAIL_FROM`
	- optional: `AIRFLOW__SMTP__SMTP_USER`, `AIRFLOW__SMTP__SMTP_PASSWORD`, `AIRFLOW__SMTP__SMTP_STARTTLS`, `AIRFLOW__SMTP__SMTP_SSL`

To actually deliver emails, make sure SMTP is configured in your Airflow environment (for example via `AIRFLOW__SMTP__SMTP_HOST`, `AIRFLOW__SMTP__SMTP_PORT`, `AIRFLOW__SMTP__SMTP_MAIL_FROM`, and optional auth/TLS settings).

## Run

Start stack (helper scripts):

```bash
./scripts/airflow/airflow_up.sh
```

Alternative direct compose:

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

## Task Sequence (Hourly DAG)

1. `validate_runtime`
2. `run_hourly_batch`
3. `wait_hourly_batch`
4. `load_batch_to_bigquery`
5. `wait_bq_load`
6. `run_dataform_models`
7. `wait_dataform_models`
8. `run_dataform_assertions`
9. `wait_dataform_assertions`

## Task Sequence (Daily Model DAG)

1. `validate_runtime`
2. `run_daily_model_refresh`
3. `wait_daily_model_refresh`

## Composer Deployment Helpers

Use helper scripts for Cloud Composer:

```bash
chmod +x scripts/gcp/composer/*.sh
```

Set Airflow Variables in Composer:

```bash
bash scripts/gcp/composer/set_composer_airflow_variables.sh
```

Upload DAGs to Composer DAG bucket:

```bash
bash scripts/gcp/composer/sync_dags_to_composer.sh
```

## Troubleshooting

See shared operations guide: [../docs/operations.md](../docs/operations.md)
