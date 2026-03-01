# Warehouse Modeling with dbt (Feature 7)

This module implements roadmap Step 7: build fact/dimension warehouse models in BigQuery from Step 6 batch outputs.

## What this dbt project builds

- Dimensions
  - `dim_transaction_type`
  - `dim_account`
  - `dim_time_hour`
- Facts
  - `fct_scored_transactions`
  - `fct_fraud_alerts`
- Mart
  - `mart_fraud_hourly_kpis`

Sources expected in BigQuery dataset `fraud_analytics` (override with dbt var `raw_dataset`):

- `curated_scored`
- `retraining_dataset`
- `monitoring_hourly`

## Prerequisites

1. Step 6 outputs are loaded into BigQuery tables above.
2. Python virtual environment is active.
3. GCP service account key exists (for BigQuery access).

Python runtime requirement for dbt:

- Use Python `3.11.x` (recommended).
- Avoid Python `3.14` for now; some dbt dependencies can fail at runtime.

Quick check:

```bash
python --version
```

Install dbt dependencies:

```bash
python3 -m pip install -r dbt/requirements.txt
```

## Configure dbt profile

1. Copy template into this project (local profile):

```bash
cp dbt/profiles.yml.example dbt/profiles.yml
```

2. Export environment variables (no hardcoded key path in profile):

```bash
export DBT_PROFILES_DIR="$PWD/dbt"
export DBT_BIGQUERY_PROJECT="<your-gcp-project-id>"
export DBT_BIGQUERY_DATASET="fraud_analytics"
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
```

Notes:

- `DBT_BIGQUERY_DATASET` is optional (default is `fraud_analytics`).
- `GOOGLE_APPLICATION_CREDENTIALS` is reused from your Spark/GCS setup.
- You can still use `~/.dbt` if you prefer, but this project-local approach avoids editing home-directory config.

## Run Feature 7

```bash
cd dbt
dbt debug
dbt deps
dbt run
dbt test
```

## Useful commands

Run only facts and marts:

```bash
dbt run --select facts marts
```

Override source dataset at runtime:

```bash
dbt run --vars '{raw_dataset: fraud_analytics}'
```

## Troubleshooting

- If `dbt run` fails with BigQuery cast errors (for example `Invalid cast from FLOAT64 to BOOL`), ensure your raw tables were loaded from latest batch parquet files. This project normalizes boolean-like values (`0/1`, `true/false`) in staging models to handle mixed source typing.
- If `dbt` crashes before running SQL (import/runtime stacktrace), verify Python version is `3.11.x` and recreate `.venv` with `python3.11 -m venv .venv`.