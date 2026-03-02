# Warehouse Modeling with dbt

## Purpose

Build warehouse dimensions, facts, and KPI marts in BigQuery from curated batch outputs.

## Models

- Dimensions: `dim_transaction_type`, `dim_account`, `dim_time_hour`
- Facts: `fct_scored_transactions`, `fct_fraud_alerts`
- Mart: `mart_fraud_hourly_kpis`

## Prerequisites

Complete shared setup first: [../docs/prerequisites.md](../docs/prerequisites.md)

Expected raw source tables in BigQuery dataset (default `fraud_analytics`):

- `curated_scored`
- `retraining_dataset`
- `monitoring_hourly`

## Setup

```bash
python3 -m pip install -r dbt/requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
```

Export runtime variables:

```bash
export DBT_PROFILES_DIR="$PWD/dbt"
export DBT_BIGQUERY_PROJECT="<your-gcp-project-id>"
export DBT_BIGQUERY_DATASET="fraud_analytics"
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
```

## Run

```bash
cd dbt
dbt debug
dbt deps
dbt run
dbt test
```

## Useful Commands

```bash
dbt run --select facts marts
dbt run --vars '{raw_dataset: fraud_analytics}'
```

## Troubleshooting

See shared operations guide: [../docs/operations.md](../docs/operations.md)
