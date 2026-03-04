# Airflow Orchestration

## Purpose

Run the orchestration pipelines in Dockerized Airflow.

Pipeline DAGs:

- `airflow/dags/fraud_hourly_orchestration.py`
- `airflow/dags/fraud_daily_model_refresh.py`

## Inputs and Outputs

- Inputs: Silver scored transactions, labels history, runtime env vars.
- Outputs: Curated parquet on GCS, BigQuery loaded tables, dbt warehouse refresh, refreshed model artifact.

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
- `DBT_BIGQUERY_PROJECT`
- `FRAUD_BIGQUERY_DATASET`
- `FRAUD_BQ_RETRAINING_TABLE`
- `FRAUD_SILVER_PATH`
- `FRAUD_BATCH_OUTPUT_BASE`
- `FRAUD_MODEL_OUTPUT`

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
3. `load_batch_to_bigquery`
4. `run_dbt_models`
5. `run_dbt_tests`

## Task Sequence (Daily Model DAG)

1. `validate_runtime`
2. `run_daily_model_refresh`

## Troubleshooting

See shared operations guide: [../docs/operations.md](../docs/operations.md)
