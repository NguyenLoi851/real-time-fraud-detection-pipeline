# Real-Time Fraud Detection Pipeline

Hybrid streaming + batch platform for fraud detection with Kafka, Spark, GCS, BigQuery, dbt, and Airflow.

Dataset source (PaySim): https://www.kaggle.com/datasets/ealaxi/paysim1

## Project Goal

- Detect suspicious transactions in near real time.
- Publish high-risk alerts to Kafka for downstream consumers.
- Persist Bronze/Silver/Gold datasets in lake storage.
- Reconcile predictions with labels in hourly batch.
- Build warehouse facts/dimensions/marts in BigQuery via dbt.
- Orchestrate hourly refresh with Airflow.

## Architecture (High Level)

![Data Pipeline Diagram](images/data-pipeline.png)

1. Simulator produces transaction events.
2. Kafka ingests raw events on `transactions_raw`.
3. Spark Structured Streaming scores events and emits:
   - lake raw/scored/alerts outputs
   - Kafka alert events on `fraud_alerts`
4. Hourly Spark batch prepares curated and monitoring/retraining datasets.
5. BigQuery load jobs ingest curated outputs.
6. dbt builds warehouse models.
7. Airflow orchestrates hourly warehouse refresh plus daily model refresh.
8. Looker Studio visualizes KPI marts.

## Documentation Entry Points

- Documentation ownership map: [docs/documentation-map.md](docs/documentation-map.md)
- Shared prerequisites: [docs/prerequisites.md](docs/prerequisites.md)
- End-to-end local runbook: [docs/runbook-local.md](docs/runbook-local.md)
- End-to-end GCP runbook: [docs/runbook-gcp.md](docs/runbook-gcp.md)
- Operations and troubleshooting: [docs/operations.md](docs/operations.md)

## Module Guides

- Data and schema notes: [data/README.md](data/README.md)
- Simulator index: [simulator/README.md](simulator/README.md)
- Streaming scoring: [streaming/README.md](streaming/README.md)
- Alert consumers: [consumers/README.md](consumers/README.md)
- Terraform foundation: [infra/terraform/README.md](infra/terraform/README.md)
- Warehouse modeling (dbt): [dbt/README.md](dbt/README.md)
- Airflow orchestration: [airflow/README.md](airflow/README.md)
- BI dashboards: [dashboards/README.md](dashboards/README.md)

## Quick Start (Local)

1. Complete setup in [docs/prerequisites.md](docs/prerequisites.md).
2. Prepare dataset in [data/README.md](data/README.md).
3. Execute [docs/runbook-local.md](docs/runbook-local.md).

## Quick Start (GCP)

1. Complete setup in [docs/prerequisites.md](docs/prerequisites.md).
2. Provision foundation with [infra/terraform/README.md](infra/terraform/README.md).
3. Execute [docs/runbook-gcp.md](docs/runbook-gcp.md).

## Documentation Rules

- Keep one canonical source per topic.
- If content is shared by 2+ modules, place it in `docs/` and link to it.
- Module READMEs contain only module-specific behavior and commands.

## 9) Future Plan (Backlog)

- [x] Refactor and simplify root `README.md` to improve clarity and reduce long sections.
- [x] Remove duplicated instructions across `README.md`, `streaming/README.md`, and simulator docs.
- [ ] Add a project FAQ/Q&A section, for example:
   - What is the difference between Fact, Dimension, and Mart tables?
   - In this pipeline, which steps are Extract / Load / Transform?
   - What are the trade-offs between sending notifications directly from Spark vs publishing alerts to Kafka and handling notifications via consumers?
   - Should we load data from GCS parquet to BigQuery with Python, or write directly from Spark to BigQuery?

      - Python BigQuery load job (recommended for this project):
         - Keep Spark focused on transformations and reliable GCS lake writes.
         - Use BigQuery native load jobs for lower cost and easier retry/operations.
         - Works well with orchestration and dbt dependencies.
      - Spark direct write to BigQuery:
         - Useful when all logic must stay in one Spark job.
         - Adds connector/runtime complexity and can be harder to operate at scale.

      Recommended pattern here: Spark writes curated parquet to GCS, then Python load jobs ingest to BigQuery, then dbt builds warehouse models.
   - How does Kafka separate data with partition ? 
   - How does group id of consumers in Kafka work ?
- [x] Add one end-to-end runbook to execute the project from start to finish (local and optional GCP path).
- [x] Add images/diagrams for end-to-end flow and major pipeline steps.
- [ ] Check logic of fraud score threshold.
