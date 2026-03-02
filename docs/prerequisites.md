# Shared Prerequisites

Use this page as the canonical setup reference for all modules.

## Runtime Requirements

- macOS/Linux shell with `bash` or `zsh`
- Python `3.11.x`
- Docker Desktop (or Docker Engine + Compose)
- Apache Spark with `spark-submit`
- Terraform `>= 1.5` (for cloud foundation)
- GCP project and service account key (for GCS/BigQuery paths)

## Python Environment (One-Time)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python --version
```

Expected output: `Python 3.11.x`

For every new terminal:

```bash
source .venv/bin/activate
```

## Kafka Local Stack (One-Time Script Permissions)

```bash
chmod +x scripts/kafka/kafka_up.sh scripts/kafka/kafka_down.sh scripts/kafka/kafka_topics.sh scripts/kafka/kafka_topic_create.sh
```

## Airflow Script Permissions

```bash
chmod +x scripts/airflow/airflow_up.sh scripts/airflow/airflow_down.sh
```

## GCP Authentication

Place your service account key at:

- `infra/terraform/keys/terraform-sa-key.json`

Export credentials before Spark/BigQuery operations on GCS:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
```

## Compatibility Notes

- Use Spark Kafka connector version matching local Spark/Scala versions.
- Keep Python at `3.11.x` for stable `dbt-bigquery` runtime.
