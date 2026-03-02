# Real-Time Fraud Detection Pipeline

Build a hybrid streaming + batch data platform to detect fraudulent transactions in real time, then improve the ML model using ground-truth labels in batch.

## 1) Project Goal

- Detect fraud in real time from transaction events.
- Score each event using an ML model (without using the original fraud label during streaming).
- Generate fraud alerts for high-risk transactions.
- Store all data in a cloud data lake and warehouse.
- Retrain the model later using true fraud labels from historical data.

## 2) End-to-End Architecture (High Level)

1. **Data Source**
	- Download dataset: https://www.kaggle.com/datasets/ealaxi/paysim1
	- Explore and understand each field before modeling.

2. **Event Simulation Layer**
	- Read each row from CSV as one transaction event.
	- Wait a small interval between rows to simulate real-time behavior.

3. **Messaging Layer (Kafka)**
	- `transactions_raw`: raw events from simulator.
	- `fraud_alerts`: only suspicious/high-risk transactions for downstream applications.

4. **Streaming Processing (Spark Structured Streaming)**
	- Consume from `transactions_raw`.
	- Apply feature transformations for online scoring.
	- Use built-in Spark ML model to predict fraud risk.
	- Persist scored transactions to data lake Silver (`scored_transactions`).
	- Publish only alert events to Kafka topic `fraud_alerts`.

5. **Storage Layer (Google Cloud Storage)**
	- Persist raw and scored events in GCS as data lake zones:
	  - Bronze: raw events
	  - Silver: cleaned/scored events
	  - Gold: curated analytics-ready data

6. **Batch Processing (Spark Hourly)**
	- Read scored transactions from Silver zone every hour.
	- Apply required transformations and build retraining-ready datasets.
	- Feed model upgrade/retraining workflows.

7. **Warehouse + Transformation (BigQuery + dbt)**
	- Load curated data to BigQuery.
	- Use dbt to build:
	  - Fact table(s): transaction facts
	  - Dimension tables: customer/account/type/time dimensions

8. **Orchestration (Airflow)**
	- Schedule ingestion, batch loads, dbt runs, model retraining, and quality checks.

9. **Visualization (Looker Studio / Data Studio)**
	- Fraud trend dashboards, alert rate, model performance, and operational KPIs.
	- Can consume/visualize from `fraud_alerts` stream and warehouse outputs.

10. **Platform & Infra**
	- Docker for local reproducible services.
	- Terraform for provisioning GCP resources.

## 3) Core Design Principle

- **During real-time scoring, do not use `isFraud` as input feature.**
- Use only available transaction attributes at event time.
- `isFraud` is used later in batch to evaluate and retrain model.

## 4) Data and ML Lifecycle

### Phase A — Dataset Research
- Download PaySim dataset.
- Build data dictionary for each column.
- Identify feature candidates and leakage risks.

### Phase B — Initial Model Training
- Train baseline fraud model on historical labeled data.
- Export model artifact for streaming inference.

### Phase C — Real-Time Inference
- Simulate events from CSV row-by-row.
- Stream through Kafka + Spark.
- Write scored transactions to Silver data lake and alerts to Kafka `fraud_alerts`.

### Phase D — Batch Truth Reconciliation
- Read scored transactions from Silver zone every hour.
- Join scored events with true labels (`isFraud`) from original dataset/history.
- Measure model quality (precision, recall, F1, AUC).
- Build retraining dataset.

### Phase E — Retraining and Deployment
- Retrain model periodically using latest labeled data.
- Validate and compare with current production model.
- Promote better model to streaming pipeline.

## 5) Suggested Repository Plan (MVP)

- `data/`: raw sample files, schema references
- `simulator/`: CSV-to-Kafka producer
- `streaming/`: Spark streaming job + online scoring logic
- `ml/`: training, evaluation, model registry artifacts
- `batch/`: reconciliation jobs and feature/label preparation
- `dbt/`: warehouse models (fact + dimensions)
- `airflow/`: DAG definitions
- `infra/terraform/`: GCP infrastructure definitions
- `docker/`: docker compose and service Dockerfiles
- `scripts/`: runnable scripts for tools/modules
	- `scripts/simulator/`: CSV simulator docker run scripts
	- `scripts/kafka/`: Kafka start/stop/topic helper scripts
- `dashboards/`: BI definitions and KPI documentation

## 5.1) Python Virtual Environment (venv)

Use Python 3.11 for this project (especially for dbt compatibility).

Create and activate virtual environment (macOS/Linux):

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Quick check after activation:

```bash
python --version
```

Expected: `Python 3.11.x`

Every time you open a new terminal, activate again:

```bash
source .venv/bin/activate
```

Deactivate when done:

```bash
deactivate
```

## 6) Execution Roadmap

1. **Data understanding**
	- Download and profile dataset.
	- Document schema, feature candidates, and leakage risks.

2. **Local event flow (Simulator + Kafka)**
	- Start simulator and Kafka with Docker.
	- Publish transaction events to `transactions_raw`.

3. **Streaming ML scoring (Spark + Kafka)**
	- Spark continuously consumes from Kafka topic `transactions_raw`.
	- Parse JSON payload into DataFrame.
	- Enforce schema, fix data types, and handle missing values.
	- Perform feature engineering (for example: `velocity_5min`, `balance_change_ratio`, `is_new_merchant`).
	- Load a pre-trained ML model and run prediction per transaction.
	- Add `fraud_score` and `predicted_is_fraud` (true/false).
	- Apply business rules for escalation.
	- Write scored output to Silver data lake (`scored_transactions`).
	- Publish only high-risk records to `fraud_alerts`.
	- Let downstream consumer services read `fraud_alerts` and send notifications (Email, Slack) or power visualization apps.

4. **Cloud foundation (Terraform + GCP)**
	- Provision base GCP resources (GCS + BigQuery foundations).

5. **Lakehouse loading**
	- Land raw/scored data in GCS (Bronze/Silver/Gold).
	- Keep this step focused on reliable storage and partitioned data layout.

6. **Hourly batch processing + model upgrade**
	- Spark batch job reads Silver `scored_transactions` every hour.
	- Apply required transformations and generate retraining/feature outputs.
	- Upgrade ML model on schedule based on latest data.
	- Load curated/transformed outputs to BigQuery.

7. **Warehouse modeling (dbt)**
	- Build fact and dimension models in BigQuery.

8. **Orchestration (Airflow)**
	- Add DAGs for batch loads, dbt runs, monitoring, and periodic retraining workflows.

9. **BI and reporting**
	- Build Looker Studio dashboard for fraud metrics and pipeline health.

## 6.1) Current Step (Now): Simulator + Kafka Local Setup

Use this section to complete roadmap **Step 2**.

1. Open simulator guide: [simulator/csv/README.md](simulator/csv/README.md)
2. Make the run script executable (first time only):
	```bash
	chmod +x scripts/simulator/run_simulator_docker.sh
	```
3. Run simulator in Docker:
	```bash
	./scripts/simulator/run_simulator_docker.sh
	```
4. Open Kafka guide for topics, producer/consumer tests, and broker checks:
	[simulator/kafka/README.md](simulator/kafka/README.md)

## 6.2) Next Step: Local PySpark Streaming ML Scoring

Use this section to complete roadmap **Step 3**.

1. Confirm prerequisites:
	- Kafka is running and receiving events on `transactions_raw`.
	- Python environment is active and dependencies installed.
	- Local Spark is installed and `spark-submit` is available.
2. Train baseline model artifact:
	```bash
	spark-submit ml/train_fraud_model.py \
	  --input data/transaction_log.csv \
	  --model-output ml/artifacts/fraud_rf_pipeline
	```
3. Create output Kafka topics:
	```bash
	./scripts/kafka/kafka_topic_create.sh fraud_alerts
	```
4. Run the streaming scoring job directly:
	```bash
	spark-submit \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
	  streaming/pyspark_fraud_streaming.py \
	  --bootstrap-servers localhost:9092 \
	  --input-topic transactions_raw \
	  --alerts-topic fraud_alerts \
	  --model-path ml/artifacts/fraud_rf_pipeline \
	  --fraud-score-threshold 0.80
	```

For full Step 3 details and runtime options, see [streaming/README.md](streaming/README.md).
For alert email consumer setup, see [consumers/README.md](consumers/README.md).

## 6.3) Next Step: Cloud Foundation (Terraform + GCP)

Use this section to complete roadmap **Step 4**.

1. Open Terraform guide:
	[infra/terraform/README.md](infra/terraform/README.md)
2. Create and download a GCP service account JSON key:
	- Go to **IAM & Admin** → **Service Accounts** in your GCP project.
	- Create a service account (for example `terraform-fraud-infra`).
	- Grant roles: `Storage Admin`, `BigQuery Admin`.
	- Open the service account **Keys** tab and create a new **JSON** key.
	- Download the key into `infra/terraform/keys/`.
3. Initialize and configure Terraform:
	```bash
	cd infra/terraform
	mkdir -p keys
	# place your downloaded key file in keys/, for example keys/terraform-sa-key.json
	cp terraform.tfvars.example terraform.tfvars
	# edit terraform.tfvars and set your project_id and service_account_key_file
	terraform init
	terraform plan
	```
4. Provision foundation resources:
	```bash
	terraform apply
	```

This creates three GCS buckets (Bronze/Silver/Gold) and one BigQuery dataset for analytics-ready tables.

## 6.4) Next Step: Lakehouse Loading on GCS (Roadmap Step 5)

After Step 4 is complete, switch data lake writes from local `data/lake/...` to GCS paths.

### 6.4.1) What is stored in each GCS bucket

- **Bronze bucket (`gs://<bronze_bucket>`):** raw/landing data and streaming checkpoints.
	- Suggested paths:
		- `gs://<bronze_bucket>/raw/transactions_raw/`
		- `gs://<bronze_bucket>/checkpoints/fraud_stream/`
- **Silver bucket (`gs://<silver_bucket>`):** cleaned + scored transaction records from Spark streaming.
	- Path:
		- `gs://<silver_bucket>/scored_transactions/`
- **Gold bucket (`gs://<gold_bucket>`):** curated alert-focused outputs used by downstream analytics.
	- Path:
		- `gs://<gold_bucket>/fraud_alerts/`

### 6.4.2) Tool input/output positions (GCS + GCP)

1. **Terraform (`infra/terraform/`)**
	- Input: `terraform.tfvars` (`project_id`, `service_account_key_file`)
	- Output: GCS bucket names + BigQuery dataset ID
2. **Simulator (`simulator/csv/realtime_csv_simulator.py`)**
	- Input: `data/realtime_transactions.csv`
	- Output: Kafka topic `transactions_raw`
3. **Kafka Producer/Topics (`simulator/kafka/*`, `scripts/kafka/*`)**
	- Input: simulator JSON events
	- Output: `transactions_raw`, `fraud_alerts`
4. **Spark Streaming (`streaming/pyspark_fraud_streaming.py`)**
	- Input: Kafka `transactions_raw` + model `ml/artifacts/fraud_rf_pipeline`
	- Output:
	  - `gs://<bronze_bucket>/raw/transactions_raw/`
	  - `gs://<silver_bucket>/scored_transactions/`
	  - `gs://<gold_bucket>/fraud_alerts/`
	  - `gs://<bronze_bucket>/checkpoints/fraud_stream/`
	  - Kafka `fraud_alerts`
5. **Alert Consumers (applications)**
	- Input: Kafka `fraud_alerts`
	- Output: real-time notifications/operational views (Email, Slack, visualization apps)
6. **Data preview tool (`streaming/read_datalake_sample.py`)**
	- Input: parquet path in Silver/Gold GCS bucket
	- Output: local console preview for validation
7. **Spark Batch (hourly)**
	- Input: `gs://<silver_bucket>/scored_transactions/`
	- Output: transformed/retraining-ready data for ML upgrade
8. **BigQuery (Step 6 output)**
	- Input: curated files from GCS Silver/Gold
	- Output: analytics-ready tables in dataset `<project_id>.fraud_analytics`

### 6.4.3) Run streaming with GCS paths

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"

spark-submit \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
	streaming/pyspark_fraud_streaming.py \
	--bootstrap-servers localhost:9092 \
	--input-topic transactions_raw \
	--alerts-topic fraud_alerts \
	--model-path ml/artifacts/fraud_rf_pipeline \
	--checkpoint-dir gs://<bronze_bucket>/checkpoints/fraud_stream \
	--datalake-raw-path gs://<bronze_bucket>/raw/transactions_raw \
	--datalake-scored-path gs://<silver_bucket>/scored_transactions \
	--datalake-alerts-path gs://<gold_bucket>/fraud_alerts \
	--fraud-score-threshold 0.80
```

Use Terraform outputs to replace `<bronze_bucket>`, `<silver_bucket>`, `<gold_bucket>`.

If you see `CANNOT_LOAD_CHECKPOINT_FILE_MANAGER` for a `gs://` checkpoint path, it usually means Spark cannot use GCS filesystem classes. Ensure:

- `GOOGLE_APPLICATION_CREDENTIALS` is exported.
- `gcs-connector` is included in `--packages`.

## 6.5) Hourly Transformation Guide (Spark Batch)

Hourly batch processing is different from streaming scoring: streaming transforms events for low-latency inference, while hourly batch transforms historical scored data for retraining, analytics, and warehouse loading.

Recommended hourly transformations:

1. **Label reconciliation**
	- Join Silver scored records with ground-truth labels (`isFraud`) from historical/source data.
	- Add fields like `is_label_available` and `label_delay_hours`.

2. **Training dataset preparation**
	- Keep rows with valid labels for supervised training.
	- Handle class imbalance (for example downsampling/weighting).
	- Apply time-based split for train/validation/test.

3. **Historical feature generation**
	- Build rolling features (1h/24h/7d): counts, sums, averages, max amounts.
	- Build ratio and behavior features (for example `amount_vs_24h_avg`).

4. **Data quality and normalization**
	- Deduplicate records using transaction/business keys.
	- Enforce schema and null/invalid value rules.
	- Normalize categorical values and ensure stable types.

5. **Monitoring transforms**
	- Produce model monitoring aggregates by hour/day (alert rate, score distribution).
	- Track drift/performance by segment (transaction type, amount bucket).

6. **Warehouse-ready outputs**
	- Write curated batch outputs for BigQuery loading.
	- Suggested outputs: scored facts, alerts facts, and optional risk profile dimensions.

### 6.5.1) Run hourly batch locally (one-shot)

Run the hourly batch job once against current Silver data and historical labels:

```bash
spark-submit batch/hourly_batch_processing.py \
	--silver-path data/lake/silver/scored_transactions \
	--labels-csv data/transaction_log.csv \
	--output-base data/lake/gold/hourly_batch \
	--model-output ml/artifacts/fraud_rf_pipeline
```

Optional: process only one UTC hour (`YYYY-MM-DD-HH`):

```bash
spark-submit batch/hourly_batch_processing.py \
	--silver-path data/lake/silver/scored_transactions \
	--labels-csv data/transaction_log.csv \
	--output-base data/lake/gold/hourly_batch \
	--model-output ml/artifacts/fraud_rf_pipeline \
	--target-hour-utc 2026-02-27-03
```

### 6.5.2) Run hourly batch on GCS

Use this when `silver-path`, `output-base`, and `model-output` are in GCS:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"

spark-submit \
	--packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28 \
	batch/hourly_batch_processing.py \
	--silver-path gs://<silver_bucket>/scored_transactions \
	--labels-csv data/transaction_log.csv \
	--output-base gs://<gold_bucket>/hourly_batch \
	--model-output gs://<gold_bucket>/models/fraud_rf_pipeline
```

Notes:

- `labels-csv` can also be a GCS path (for example `gs://<bronze_bucket>/reference/transaction_log.csv`).
- Model refresh trains only on labeled records that exist in scored transaction history, then overwrites `model-output`.
- If Spark cannot read/write `gs://` paths, verify `GOOGLE_APPLICATION_CREDENTIALS` and `gcs-connector` package.

### 6.5.3) Load hourly batch outputs from GCS to BigQuery

Run this after hourly batch finishes writing to GCS:

```bash
python3 batch/load_hourly_batch_to_bigquery.py \
	--project-id <your-gcp-project-id> \
	--dataset fraud_analytics \
	--gcs-output-base gs://<gold_bucket>/hourly_batch \
	--write-disposition WRITE_TRUNCATE \
	--create-dataset-if-missing
```

Optional: load only selected tables:

```bash
python3 batch/load_hourly_batch_to_bigquery.py \
	--project-id <your-gcp-project-id> \
	--dataset fraud_analytics \
	--gcs-output-base gs://<gold_bucket>/hourly_batch \
	--tables curated_scored monitoring_hourly
```

Outputs:

- `data/lake/gold/hourly_batch/curated_scored` (or `gs://<gold_bucket>/hourly_batch/curated_scored`)
- `data/lake/gold/hourly_batch/retraining_dataset` (or `gs://<gold_bucket>/hourly_batch/retraining_dataset`)
- `data/lake/gold/hourly_batch/monitoring_hourly` (or `gs://<gold_bucket>/hourly_batch/monitoring_hourly`)

## 6.6) Next Step: Warehouse Modeling with dbt (Roadmap Step 7)

Feature 7 is implemented in `dbt/` and builds BigQuery warehouse models:

- Dimensions: `dim_transaction_type`, `dim_account`, `dim_time_hour`
- Facts: `fct_scored_transactions`, `fct_fraud_alerts`
- Mart: `mart_fraud_hourly_kpis`

1. Open warehouse guide:
	[dbt/README.md](dbt/README.md)
2. Install dbt adapter:
	```bash
	python3 -m pip install -r dbt/requirements.txt
	```
3. Configure profile:
	```bash
	cp dbt/profiles.yml.example dbt/profiles.yml
	export DBT_PROFILES_DIR="$PWD/dbt"
	export DBT_BIGQUERY_PROJECT="<your-gcp-project-id>"
	export DBT_BIGQUERY_DATASET="fraud_analytics"
	export GOOGLE_APPLICATION_CREDENTIALS="$PWD/infra/terraform/keys/terraform-sa-key.json"
	```
4. Run dbt models + tests:
	```bash
	cd dbt
	dbt debug
	dbt run
	dbt test
	```

## 6.7) Next Step: Orchestration with Airflow (Roadmap Step 8)

Step 8 is implemented in `airflow/` and automates hourly batch/retraining + warehouse refresh.

Pipeline order in DAG `fraud_hourly_batch_and_warehouse`:

1. `batch/hourly_batch_processing.py` (hourly Spark batch + model refresh)
2. `batch/load_hourly_batch_to_bigquery.py` (GCS parquet → BigQuery)
3. `dbt run` + `dbt test` (warehouse models and checks)

1. Open orchestration guide:
	[airflow/README.md](airflow/README.md)
2. Configure Dockerized Airflow env:
	```bash
	cp airflow/.env.example airflow/.env
	# edit airflow/.env with your project/bucket values
	```
3. Start Airflow stack:
	```bash
	chmod +x scripts/airflow/airflow_up.sh scripts/airflow/airflow_down.sh
	./scripts/airflow/airflow_up.sh
	```
4. Open `http://localhost:8080` and enable DAG:
	- `fraud_hourly_batch_and_warehouse`

## 6.8) Next Step: BI and Reporting (Roadmap Step 9)

Use this after Step 8 is enabled, or run manually if orchestration is paused.

1. Ensure BigQuery tables/marts are available (Step 6 + Step 7 outputs).
2. Open BI guide:
	[dashboards/README.md](dashboards/README.md)
3. Build Looker Studio report using BigQuery source:
	- `fraud_analytics.mart_fraud_hourly_kpis`
4. Add core visuals:
	- hourly transaction volume
	- alert volume/rate trend
	- fraud score trend and p95 score
	- breakdown by `transaction_type`

Note: if Airflow is paused, refresh data manually by running hourly batch + BigQuery load + dbt marts.

## 7) Minimal Success Criteria (MVP)

- Real-time transaction events flow from simulator → Kafka → Spark scoring.
- Alerts generated for suspicious transactions and published to `fraud_alerts`.
- Raw/scored data stored in GCS.
- Hourly Spark batch reads `scored_transactions` and prepares model upgrade/retraining inputs.
- Curated fact/dimension tables built in BigQuery via dbt.
- Airflow automates scheduled batch/retraining pipeline.
- Dashboard shows fraud trends and model performance.

## 8) Key Risks to Watch Early

- Feature leakage (accidentally using `isFraud` or future info online).
- Data drift between historical and simulated/streaming data.
- Late-arriving labels and mismatch during reconciliation.
- Cost/performance tuning for Spark, BigQuery, and storage.

## 9) Future Plan (Backlog)

- [ ] Refactor and simplify root `README.md` to improve clarity and reduce long sections.
- [ ] Remove duplicated instructions across `README.md`, `streaming/README.md`, and simulator docs.
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
- [ ] Add one end-to-end runbook to execute the project from start to finish (local and optional GCP path).
- [ ] Add images/diagrams for end-to-end flow and major pipeline steps.
- [ ] Add a document listing all related tools/services used in the project and where each is used.
