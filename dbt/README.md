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
cd dbt                  # Navigate into the dbt project directory

dbt debug               # Validate your connection and profile configuration
dbt deps                # Install dbt packages listed in packages.yml
dbt run                 # Compile and execute all models in BigQuery
dbt test                # Run schema and data quality tests on all models
```

## Useful Commands

```bash
# Run only fact and mart models (skips dimensions)
dbt run --select facts marts

# Override the raw source dataset at runtime
dbt run --vars '{raw_dataset: fraud_analytics}'
```

## Docs
```bash
# Generate static documentation site from your models and schema files
dbt docs generate

# Serve the documentation locally and open it in your browser (default: http://localhost:8080)
dbt docs serve
```

## Troubleshooting

See shared operations guide: [../docs/operations.md](../docs/operations.md)
