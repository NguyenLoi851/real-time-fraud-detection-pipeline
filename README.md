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
	- `transactions_scored`: scored events with fraud probability/prediction.
	- `fraud_alerts`: only suspicious/high-risk transactions.

4. **Streaming Processing (Spark Structured Streaming)**
	- Consume from `transactions_raw`.
	- Apply feature transformations for online scoring.
	- Use built-in Spark ML model to predict fraud risk.
	- Publish scored records and alerts to Kafka topics.

5. **Storage Layer (Google Cloud Storage)**
	- Persist raw and scored events in GCS as data lake zones:
	  - Bronze: raw events
	  - Silver: cleaned/scored events
	  - Gold: curated analytics-ready data

6. **Warehouse + Transformation (BigQuery + dbt)**
	- Load curated data to BigQuery.
	- Use dbt to build:
	  - Fact table(s): transaction facts
	  - Dimension tables: customer/account/type/time dimensions

7. **Orchestration (Airflow)**
	- Schedule ingestion, batch loads, dbt runs, model retraining, and quality checks.

8. **Visualization (Looker Studio / Data Studio)**
	- Fraud trend dashboards, alert rate, model performance, and operational KPIs.

9. **Platform & Infra**
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
- Write predictions and alerts.

### Phase D — Batch Truth Reconciliation
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

Create and activate virtual environment (macOS/Linux):

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

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
	- Write scored output to data lake.
	- Publish scored records to `scored-transactions` and high-risk records to `fraud-alerts`.
	- Trigger email alerts when `fraud_score` is above threshold.

4. **Cloud foundation (Terraform + GCP)**
	- Provision base GCP resources (GCS + BigQuery foundations).

5. **Lakehouse loading**
	- Land raw/scored data in GCS and load curated outputs to BigQuery.

6. **Warehouse modeling (dbt)**
	- Build fact and dimension models in BigQuery.

7. **Orchestration (Airflow)**
	- Add DAGs for batch loads, dbt runs, and retraining workflows.

8. **Monitoring and retraining**
	- Add model monitoring and periodic retraining.

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
	./scripts/kafka/kafka_topic_create.sh scored-transactions
	./scripts/kafka/kafka_topic_create.sh fraud-alerts
	```
4. Run the streaming scoring job directly:
	```bash
	spark-submit \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
	  streaming/pyspark_fraud_streaming.py \
	  --bootstrap-servers localhost:9092 \
	  --input-topic transactions_raw \
	  --scored-topic scored-transactions \
	  --alerts-topic fraud-alerts \
	  --model-path ml/artifacts/fraud_rf_pipeline \
	  --fraud-score-threshold 0.80
	```

For full Step 3 details, options, and email alert setup, see [streaming/README.md](streaming/README.md).

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

## 7) Minimal Success Criteria (MVP)

- Real-time transaction events flow from simulator → Kafka → Spark scoring.
- Alerts generated for suspicious transactions.
- Raw/scored data stored in GCS.
- Curated fact/dimension tables built in BigQuery via dbt.
- Airflow automates scheduled batch/retraining pipeline.
- Dashboard shows fraud trends and model performance.

## 8) Key Risks to Watch Early

- Feature leakage (accidentally using `isFraud` or future info online).
- Data drift between historical and simulated/streaming data.
- Late-arriving labels and mismatch during reconciliation.
- Cost/performance tuning for Spark, BigQuery, and storage.
