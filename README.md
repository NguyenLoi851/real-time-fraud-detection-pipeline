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
- `dashboards/`: BI definitions and KPI documentation

## 6) Execution Roadmap (Not Too Detailed)

1. Set up local dev with Docker (Kafka, Spark, Airflow) and create Terraform skeleton for GCP.
2. Download and profile dataset; document schema and feature plan.
3. Build CSV event simulator and publish to Kafka raw topic.
4. Build Spark streaming scoring job and alert topic flow.
5. Land raw/scored data in GCS and load curated outputs to BigQuery.
6. Build dbt fact/dimension models in BigQuery.
7. Create Airflow DAGs for orchestration (stream start, batch jobs, dbt, retrain).
8. Add model monitoring + periodic retraining workflow.
9. Build Looker Studio dashboard for fraud metrics and pipeline health.

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

---

This README is your implementation plan and architecture baseline. Start with MVP, then iterate by improving model quality, observability, and scale.
