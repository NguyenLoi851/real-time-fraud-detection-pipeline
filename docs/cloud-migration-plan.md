# Cloud Migration Plan (Cloud-Native Target on GCP)

This document defines how to evolve this repository into a cloud-native implementation on GCP.

Scope of this plan:

- The new implementation targets cloud runtime only.
- Legacy local/Docker behavior is preserved as a separate version line (tag/branch), not as an active compatibility target in this plan.

---

## 1) Current vs Target Mapping

| Current Component | Current Runtime | GCP Alternative | Notes |
|---|---|---|---|
| Event bus | Kafka (Docker) | Pub/Sub | Replace topic semantics + consumer groups with subscription model |
| Stream scoring | Local Spark Structured Streaming | Dataproc Serverless Spark (or Dataflow in future) | Keep PySpark logic first, change source/sink adapters |
| Batch reconciliation | Spark batch local/Docker | Dataproc Serverless batch | Reuse current Spark jobs with cloud packaging |
| Orchestration | Airflow in Docker | Cloud Composer | Migrate DAGs with minimal behavior change |
| Warehouse transform | dbt BigQuery | Dataform (BigQuery-native) | Dataform is the cloud-track transform engine; dbt remains only in archived legacy line |
| Alerts consumer | Python Kafka consumer | Local Pub/Sub pull worker | Stateless email/webhook worker |
| Data lake storage | Local filesystem + GCS | GCS | Bronze/Silver/Gold remains valid |
| Serving warehouse | BigQuery | BigQuery | No change |

---

## 2) Migration Principles

1. Optimize for cloud-native reliability and operations.
2. Use adapter boundaries, not large rewrites.
3. Keep table contracts stable (curated/retraining/monitoring outputs).
4. Separate legacy local runtime by versioning strategy (tag/branch), not runtime switches.
5. Validate parity against baseline outputs at each phase (record counts, schema, KPI deltas, alert volume).

---

## 3) Release and Versioning Strategy

Recommended production-friendly approach:

1. Keep current local implementation in a frozen legacy line:
   - Tag example: `v1.0-local-legacy`
   - Optional maintenance branch: `release/local-legacy`
2. Build cloud-native implementation on active development line:
   - Branch example: `main` (or `develop/cloud-native` then merge to `main`)
3. Use release tags for cloud milestones:
   - `v2.0-cloud-foundation`
   - `v2.1-composer-dataproc`
  - `v2.2-dataform`

Why this is standard:

- Clear separation of support policies.
- No runtime branching complexity in production code.
- Easier rollback and release notes.

---

## 4) Recommended Repository Strategy

Keep existing directories, add cloud service adapters and deployment packaging.

Proposed additions:

- `streaming/adapters/`:
  - `pubsub_io.py` (new)
- `consumers/`:
  - add `alert_pubsub_consumer.py` for GCP mode
- `orchestration/`:
  - add Composer-ready DAG package and deployment notes
- `warehouse/` (docs + config only at first):
  - add `dataform/` as cloud-track source of truth
  - keep dbt assets out of active cloud runtime paths

Why this structure works:

- Contributors can migrate one module at a time.
- CI can enforce cloud deployment checks directly.

---

## 5) Phased Implementation Plan

## Phase 1: Messaging Migration (Kafka -> Pub/Sub) (3-5 days)

Deliverables:

- Add publisher path for transaction events to Pub/Sub topic.
- Add subscriber worker for fraud alerts (local Pub/Sub pull).
- Remove Kafka dependency from the cloud code path.

Implementation notes:

- Use `google-cloud-pubsub` in cloud runtime.
- Use a shared event envelope schema to keep downstream compatibility.

Exit criteria:

- End-to-end alert flow works on Pub/Sub + local pull worker.

## Phase 2: Spark Runtime Migration (local Spark -> Dataproc Serverless) (4-7 days)

Deliverables:

- Package current Spark jobs for Dataproc Serverless batch submissions:
  - `streaming/pyspark_fraud_streaming.py` (micro-batch pattern)
  - `batch/hourly_batch_processing.py`
  - `batch/daily_model_refresh.py`
- Move checkpoints and model artifacts to GCS in gcp-native mode.
- Add run scripts under `scripts/gcp/dataproc/`.

Implementation notes:

- Preserve feature engineering and model scoring logic.
- Isolate source/sink differences in adapter functions.
- Ensure service account and IAM scopes are documented.

Exit criteria:

- Hourly and daily jobs execute on Dataproc Serverless and produce expected outputs in GCS/BigQuery.

## Phase 3: Warehouse Evolution (Dataform-First) (5-10 days)

Deliverables:

- Create equivalent Dataform project for a subset first:
  - dimensions
  - one fact
  - one mart
- Expand Dataform conversion to full required production model set.
- Compare outputs (row count + key metric parity) during each conversion wave.

Implementation notes:

- All new and migrated warehouse logic is implemented only in Dataform.
- Keep model naming and contract compatibility for downstream consumers during cutover.

Decision gate:

- No dual-engine decision path for cloud track: proceed with full Dataform migration.
- Gate focuses on readiness to advance to Composer orchestration (parity, quality checks, and runbook completeness).

Exit criteria:

- Dataform produces parity-validated outputs for required production model scope.

## Phase 4: Orchestration Migration (Docker Airflow -> Composer) (3-5 days)

Deliverables:

- Port current DAGs to Composer-compatible deployment:
  - `airflow/dags/fraud_hourly_orchestration.py`
  - `airflow/dags/fraud_daily_model_refresh.py`
- Replace local path assumptions with GCS/Composer env variables.
- Replace dbt CLI task logic with Dataform workflow invocation pattern.
- Add Composer deployment script and operations runbook.

Implementation notes:

- Keep task order identical to reduce regression risk.
- First run Composer DAGs against Dataform targets in a staged environment.

Exit criteria:

- Scheduled runs succeed in Composer for hourly and daily pipelines using Dataform-backed transforms.

## Phase 5: Documentation and Contributor Experience (2-3 days)

Deliverables:

- Add clear mode selector in root documentation.
- Add cloud-native quickstart and deployment checklist.
- Add legacy version selection note (tag/branch strategy).
- Add troubleshooting for cloud runtime only in the new version line.
- Add cost notes for managed services.

Exit criteria:

- New contributor can deploy and run the cloud-native pipeline from docs only.

---

## 6) Configuration Model (Single Source of Runtime Truth)

Use standardized env variables across modules:

- `PIPELINE_MODE=gcp-native`
- `MESSAGE_BACKEND=pubsub`
- `ORCHESTRATOR_BACKEND=composer`
- `SPARK_BACKEND=dataproc-serverless`
- `TRANSFORM_BACKEND=dataform`

Guideline:

- Runtime selection must happen in wrapper scripts and DAG params.

---

## 7) Testing and Parity Plan

Minimum validation suite per phase:

1. Contract tests (schema + required columns).
2. Data parity checks for one fixed hourly window.
3. DAG task success checks with expected artifacts present.
4. Dataform model quality checks (or equivalent assertions).

Recommended acceptance thresholds:

- Row-count delta per hourly table: <= 0.5%
- KPI delta on fraud rate: <= 1% relative difference
- Zero missing required columns

---

## 8) Risks and Mitigations

1. Messaging semantics drift (Kafka groups vs Pub/Sub subscriptions).
   Mitigation: explicit subscription strategy per consumer role.
2. Spark connector/runtime differences in cloud.
   Mitigation: lock connector versions and add smoke tests on Dataproc.
3. Dataform conversion scope and parity drift risk.
  Mitigation: phase conversion by domain, enforce parity thresholds per wave, and block Composer handoff until parity checks pass.
4. Cost increase from always-on managed services.
   Mitigation: start with scheduled/serverless jobs and budget alerts.

---

## 9) First Build Backlog (Implementation Order)

1. Add GCP config profile and runtime variables.
2. Add Pub/Sub producer and alert consumer path.
3. Add Dataproc submission scripts for hourly/daily jobs.
4. Add Dataform skeleton and first model parity checks.
5. Add Composer environment + DAG deployment path wired to Dataform workflows.
6. Publish cloud-native quickstart and legacy-version selection note.

---

## 10) Definition of Done (Cloud Track)

Cloud track is complete when:

- A contributor can deploy and run cloud-native mode on GCP with documented setup.
- Hourly and daily orchestration run in Composer.
- Data lake and warehouse outputs pass parity checks.
- Documentation clearly explains cloud deployment and legacy version access.

---

## 11) Phase 1 Implementation (Completed in Repository)

Phase 1 has been implemented with the following repository additions:

- Pub/Sub event envelope adapter:
  - `streaming/adapters/pubsub_io.py`
- Pub/Sub transaction publisher:
  - `simulator/pubsub/pubsub_csv_publisher.py`
- Local Pub/Sub pull alert consumer (email/webhook delivery):
  - `consumers/alert_pubsub_consumer.py`

Behavior notes:

- The cloud code path now supports Pub/Sub messaging without Kafka dependencies.
- Event payloads use a shared envelope shape:
  - `schema_version`
  - `event_id`
  - `event_type`
  - `source`
  - `emitted_at_utc`
  - `payload`

---

## 12) Detailed GCP Setup and Operations Guide for Phase 1 (Pub/Sub + Local Alert Worker)

This section is the step-by-step operator guide for setting up, testing, and troubleshooting Phase 1 messaging services on GCP with a local alert consumer.

### 12.1 Prerequisites

1. Local tools:
   - `gcloud` CLI installed and authenticated.
   - Python 3.11+ and project dependencies installed.
2. Permissions on target GCP project:
   - Project IAM admin-level role (or equivalent granular roles) for initial setup.
3. Required APIs enabled:
   - `pubsub.googleapis.com`

### 12.2 Set Working Environment Variables

Set once per shell session:

```bash
export GCP_PROJECT_ID="<your-project-id>"
export GCP_REGION="us-central1"
export PUBSUB_TRANSACTIONS_TOPIC="transactions_raw"
export PUBSUB_FRAUD_ALERTS_TOPIC="fraud_alerts"
export TRANSACTIONS_PULL_SUBSCRIPTION="transactions-raw-pull-sub"
export ALERT_PULL_SUBSCRIPTION="fraud-alerts-pull-sub"
```

Initialize gcloud context:

```bash
gcloud config set project "$GCP_PROJECT_ID"
```

### 12.3 Enable Required GCP Services

```bash
gcloud services enable pubsub.googleapis.com
```

### 12.4 Create Pub/Sub Topics and Subscription

```bash
gcloud pubsub topics create "$PUBSUB_TRANSACTIONS_TOPIC"
gcloud pubsub subscriptions create "$TRANSACTIONS_PULL_SUBSCRIPTION" \
  --topic="$PUBSUB_TRANSACTIONS_TOPIC"

gcloud pubsub topics create "$PUBSUB_FRAUD_ALERTS_TOPIC"
gcloud pubsub subscriptions create "$ALERT_PULL_SUBSCRIPTION" \
  --topic="$PUBSUB_FRAUD_ALERTS_TOPIC"
```

Optional inspection:

```bash
gcloud pubsub topics list
gcloud pubsub subscriptions describe "$ALERT_PULL_SUBSCRIPTION"
```

### 12.4.1 Grant IAM Roles to the Runtime Identity

If the local consumer uses a service account key from `GOOGLE_APPLICATION_CREDENTIALS`, grant Pub/Sub roles to that service account.

Set variables:

```bash
export SA_EMAIL="<service-account-email-in-credential-file>"
```

Grant subscriber role for local pull consumer:

```bash
gcloud pubsub subscriptions add-iam-policy-binding "$ALERT_PULL_SUBSCRIPTION" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.subscriber"
```

If the same identity publishes test alerts, grant publisher role:

```bash
gcloud pubsub topics add-iam-policy-binding "$PUBSUB_FRAUD_ALERTS_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher"
```

If the same identity creates/deletes topics or subscriptions, grant editor role:

```bash
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.editor"
```

### 12.5 Run Local Alert Consumer

```bash
python3 consumers/alert_pubsub_consumer.py \
  --pubsub-project-id "$GCP_PROJECT_ID" \
  --pubsub-subscription "$ALERT_PULL_SUBSCRIPTION" \
  --delivery-mode email \
  --email-use-tls
```

### 12.5.1 Part 3 Check: Ensure Subscriber Role on Pull Subscription

Before publishing test traffic, ensure the runtime identity has all required Pub/Sub permissions:

```bash
gcloud pubsub topics add-iam-policy-binding "$PUBSUB_TRANSACTIONS_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher"

gcloud pubsub topics add-iam-policy-binding "$PUBSUB_FRAUD_ALERTS_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher"

gcloud pubsub subscriptions add-iam-policy-binding "$ALERT_PULL_SUBSCRIPTION" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.subscriber"
```

### 12.6 Publish Transaction Events to Pub/Sub

Use the new publisher:

```bash
python3 simulator/pubsub/pubsub_csv_publisher.py \
  --input data/transaction_log.csv \
  --project-id "$GCP_PROJECT_ID" \
  --topic "$PUBSUB_TRANSACTIONS_TOPIC" \
  --interval-min 0.1 \
  --interval-max 0.4 \
  --max-events 100
```

At this phase, this validates transaction publish path.

### 12.7 Publish Fraud Alert Test Events

Example one-off alert publish command:

```bash
gcloud pubsub topics publish "$PUBSUB_FRAUD_ALERTS_TOPIC" \
  --message='{"schema_version":"1.0","event_id":"manual-test-1","event_type":"fraud.alert","source":"manual.test","emitted_at_utc":"2026-01-01T00:00:00Z","payload":{"type":"TRANSFER","nameOrig":"C_TEST","nameDest":"C_DEST","amount":99999.99,"fraud_score":0.99,"predicted_is_fraud":true,"is_alert":true,"event_ts":"2026-01-01T00:00:00Z"}}' \
  --attribute=event_type=fraud.alert,source=manual.test,event_id=manual-test-1
```

Expected result:

- Local consumer receives the message from `$ALERT_PULL_SUBSCRIPTION`.
- Terminal logs show processed alert.
- Email or webhook is delivered based on `ALERT_DELIVERY_MODE`.

### 12.8 Operational Tips

1. Configure Pub/Sub dead-letter topics for production-grade error isolation.
2. Add idempotency key handling using `event_id` in downstream notification processing.
3. Use Secret Manager for SMTP credentials in production (avoid plain env vars).

### 12.9 Common Failure Cases and Fixes

1. Email send failure:
   - Validate SMTP host/port/TLS and account app-password requirements.
2. No message delivery observed:
   - Check topic/subscription names and confirm the local consumer is running.
3. Messages keep reappearing:
   - Review `ALERT_PULL_ACK_ON_ERROR` and investigate processing errors in terminal logs.

### 12.10 Cleanup Commands (Optional)

```bash
gcloud pubsub subscriptions delete "$ALERT_PULL_SUBSCRIPTION"
gcloud pubsub topics delete "$PUBSUB_FRAUD_ALERTS_TOPIC"
gcloud pubsub topics delete "$PUBSUB_TRANSACTIONS_TOPIC"
```

---

## 13) Phase 2 Implementation (Completed in Repository)

Phase 2 is now implemented with Dataproc Serverless-ready Spark entrypoints and submit wrappers.

Repository additions and updates:

- Spark jobs now support `--runtime-mode local|gcp-native` and do not force `local[*]` in cloud mode:
  - `batch/hourly_batch_processing.py`
  - `batch/daily_model_refresh.py`
  - `streaming/pyspark_fraud_streaming.py`
- Daily model refresh now reads BigQuery through the Spark BigQuery connector when `--runtime-mode gcp-native` is used.
- Dataproc Serverless submit scripts were added under:
  - `scripts/gcp/dataproc/submit_hourly_batch.sh`
  - `scripts/gcp/dataproc/submit_hourly_bq_load.sh`
  - `scripts/gcp/dataproc/submit_daily_model_refresh.sh`
  - `scripts/gcp/dataproc/submit_streaming_batch.sh`

Behavior notes:

- Cloud mode uses Dataproc ADC for GCS access, so `GOOGLE_APPLICATION_CREDENTIALS` is optional on Dataproc Serverless.
- The hourly and daily jobs are the primary Phase 2 Dataproc workloads.
- The streaming script now consumes from Pub/Sub subscription input and publishes fraud alerts to a Pub/Sub topic in cloud-native mode.

---

## 14) Detailed GCP Setup and Operations Guide for Phase 2 (Dataproc Serverless Spark)

This section is the step-by-step operator guide for running the Phase 2 Spark workloads on GCP with Dataproc Serverless.

### 14.1 Prerequisites

1. Local tools:
  - `gcloud` CLI installed and authenticated.
  - Bash shell available on macOS or Linux.
2. GCP permissions on the target project:
  - Dataproc Serverless job submission.
  - BigQuery read access for the model refresh job.
  - Storage access to the bucket used for model artifacts and lake outputs.
3. Required APIs enabled:
  - `dataproc.googleapis.com`
  - `bigquery.googleapis.com`
  - `storage.googleapis.com`

### 14.2 Set Working Environment Variables

Run this section in order:

1. Use **14.2.1** to decide/create each value.
2. After values are ready, run the export block in **14.2.2**.

### 14.2.1 How to Choose/Create Each Value

Use this section if any variable value is unclear.

1. `GCP_PROJECT_ID`
  - Your GCP project id.
  - Find it with:

```bash
gcloud projects list
```

2. `GCP_REGION`
  - Region where Dataproc Serverless batches run.
  - Must match where your subnet exists (if subnet is set).

3. `GCP_GCS_BUCKET`
  - Root bucket used by Phase 2 paths (`lake/`, `checkpoints/`, `ml/artifacts/`).

3.1 `GCP_DATAPROC_DEPS_BUCKET`
  - GCS bucket used by `gcloud dataproc batches submit` to stage dependencies.
  - Set this to `GCP_GCS_BUCKET` unless you want a separate staging bucket.
  - This avoids the error: `--deps-bucket was not specified`.

4. `GCP_DATAPROC_SERVICE_ACCOUNT`
  - Runtime service account email used by Dataproc batches.
  - Format:
  - `<service-account-name>@<project-id>.iam.gserviceaccount.com`
  - Use one fixed service account name for the whole runbook, for example:

```bash
export SA_NAME="dataproc-fraud-runtime"
export GCP_DATAPROC_SERVICE_ACCOUNT="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
```

  - The account creation step happens later in **14.3.1** using this same value.

5. `GCP_DATAPROC_SUBNET`
  - Subnet resource path where Dataproc Serverless attaches network interfaces.
  - Format:
  - `projects/<project-id>/regions/<region>/subnetworks/<subnet-name>`

List existing subnets:

```bash
gcloud compute networks subnets list \
  --project "$GCP_PROJECT_ID" \
  --regions "$GCP_REGION"
```

Set existing subnet value:

```bash
export GCP_DATAPROC_SUBNET="projects/${GCP_PROJECT_ID}/regions/${GCP_REGION}/subnetworks/<subnet-name>"
```

If you do not have a subnet yet, create one:

```bash
gcloud compute networks create fraud-vpc \
  --project "$GCP_PROJECT_ID" \
  --subnet-mode=custom

gcloud compute networks subnets create fraud-dataproc-subnet \
  --project "$GCP_PROJECT_ID" \
  --region "$GCP_REGION" \
  --network fraud-vpc \
  --range 10.10.0.0/24 \
  --enable-private-ip-google-access

export GCP_DATAPROC_SUBNET="projects/${GCP_PROJECT_ID}/regions/${GCP_REGION}/subnetworks/fraud-dataproc-subnet"
```

If you see this warning after creating the VPC, it is expected for custom networks:

`Instances on this network will not be reachable until firewall rules are created.`

For this Dataproc Serverless Phase 2 flow:

- You usually do not need SSH/RDP ingress rules.
- Dataproc Serverless does not require direct VM login for these jobs.
- Prioritize subnet + IAM + bucket/dataset permissions first.

Optional internal firewall rule (safe baseline for internal traffic inside the subnet):

```bash
gcloud compute firewall-rules create fraud-allow-internal \
  --project "$GCP_PROJECT_ID" \
  --network fraud-vpc \
  --direction INGRESS \
  --priority 1000 \
  --action ALLOW \
  --rules tcp,udp,icmp \
  --source-ranges 10.10.0.0/24
```

You can leave this variable empty to let Dataproc use default networking, but explicit subnet is recommended for production.

6. `RUN_TS` and batch id variables (`GCP_DATAPROC_HOURLY_BATCH_ID`, `GCP_DATAPROC_DAILY_BATCH_ID`, `GCP_DATAPROC_STREAMING_BATCH_ID`)
  - `RUN_TS` is a timestamp string used to generate unique Dataproc batch ids.
  - Format from `date +%Y%m%d%H%M%S`: `YYYYMMDDHHMMSS`.
  - Example: `20260407143055`.

Generate once and reuse for all 3 job ids:

```bash
export RUN_TS="$(date +%Y%m%d%H%M%S)"
export GCP_DATAPROC_HOURLY_BATCH_ID="fraud-hourly-batch-${RUN_TS}"
export GCP_DATAPROC_BQ_LOAD_BATCH_ID="fraud-hourly-bq-load-${RUN_TS}"
export GCP_DATAPROC_DAILY_BATCH_ID="fraud-daily-model-refresh-${RUN_TS}"
export GCP_DATAPROC_STREAMING_BATCH_ID="fraud-streaming-batch-${RUN_TS}"
```

If you run jobs again later, regenerate `RUN_TS` first so ids stay unique:

```bash
export RUN_TS="$(date +%Y%m%d%H%M%S)"
```

7. `PIPELINE_MODE`
  - Use `gcp-native` for Dataproc/Cloud mode.

8. `FRAUD_SHUFFLE_PARTITIONS`
  - Spark shuffle partition hint for these jobs.
  - Start with `8`; tune based on data volume and job runtime.

9. `BIGQUERY_CONNECTOR_PACKAGE`
  - Optional Maven coordinate override for Spark BigQuery connector used by daily model refresh.
  - Default behavior uses the Dataproc Serverless built-in BigQuery connector (recommended).

10. `FRAUD_SILVER_PATH`, `FRAUD_LABELS_CSV`, `FRAUD_HOURLY_OUTPUT_BASE`, `FRAUD_MODEL_OUTPUT`
  - Paths under `gs://${GCP_GCS_BUCKET}` used by hourly and daily jobs.
  - Keep defaults unless you want separate prefixes/environments.

11. `FRAUD_BQ_DATASET`, `FRAUD_RETRAINING_TABLE`
  - BigQuery dataset and table read by daily model refresh.
  - Defaults match this repo's Phase 2 implementation.

### 14.2.2 Export Variables After Values Are Ready

Run this block only after you complete **14.2.1**:

```bash
export GCP_PROJECT_ID="<your-project-id>"
export GCP_REGION="us-central1"
export GCP_GCS_BUCKET="<your-lake-and-artifacts-bucket>"
export GCP_DATAPROC_DEPS_BUCKET="${GCP_GCS_BUCKET}"
export SA_NAME="dataproc-fraud-runtime"
export GCP_DATAPROC_SERVICE_ACCOUNT="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
export GCP_DATAPROC_SUBNET="<optional-vpc-subnet-uri>"
export RUN_TS="$(date +%Y%m%d%H%M%S)"
export GCP_DATAPROC_HOURLY_BATCH_ID="fraud-hourly-batch-${RUN_TS}"
export GCP_DATAPROC_BQ_LOAD_BATCH_ID="fraud-hourly-bq-load-${RUN_TS}"
export GCP_DATAPROC_DAILY_BATCH_ID="fraud-daily-model-refresh-${RUN_TS}"
export GCP_DATAPROC_STREAMING_BATCH_ID="fraud-streaming-batch-${RUN_TS}"

export PIPELINE_MODE="gcp-native"
export FRAUD_SHUFFLE_PARTITIONS="8"
# Optional: only set this if you intentionally override Dataproc's built-in connector.
# export BIGQUERY_CONNECTOR_PACKAGE="com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1"

export FRAUD_SILVER_PATH="gs://${GCP_GCS_BUCKET}/lake/silver/scored_transactions"
export FRAUD_LABELS_CSV="gs://${GCP_GCS_BUCKET}/inputs/transaction_log.csv"
export FRAUD_HOURLY_OUTPUT_BASE="gs://${GCP_GCS_BUCKET}/lake/gold/hourly_batch"

export FRAUD_MODEL_OUTPUT="gs://${GCP_GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline"
export FRAUD_BQ_DATASET="fraud_analytics"
export FRAUD_RETRAINING_TABLE="retraining_dataset"
```

For the streaming batch wrapper (Pub/Sub source), also set:

```bash
export FRAUD_INPUT_SUBSCRIPTION="transactions-raw-pull-sub"
export FRAUD_ALERTS_TOPIC="fraud_alerts"
export FRAUD_MODEL_PATH="gs://${GCP_GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline"
export FRAUD_RAW_PATH="gs://${GCP_GCS_BUCKET}/lake/bronze/transactions_raw"
export FRAUD_SCORED_PATH="gs://${GCP_GCS_BUCKET}/lake/silver/scored_transactions"
export FRAUD_ALERTS_PATH="gs://${GCP_GCS_BUCKET}/lake/gold/fraud_alerts"
```

Initialize gcloud context:

```bash
gcloud config set project "$GCP_PROJECT_ID"
gcloud config set dataproc/region "$GCP_REGION"
```

### 14.3 Enable Required GCP Services

```bash
gcloud services enable dataproc.googleapis.com bigquery.googleapis.com storage.googleapis.com pubsub.googleapis.com
```

### 14.3.1 Create and Grant IAM Roles for Dataproc Runtime Service Account

Create a dedicated service account for Dataproc Serverless jobs and grant only the roles required for Phase 2.

Use the same values already set in **14.2.2**:

```bash
export SA_EMAIL="$GCP_DATAPROC_SERVICE_ACCOUNT"
```

Create the service account:

```bash
gcloud iam service-accounts create "$SA_NAME" \
  --project "$GCP_PROJECT_ID" \
  --display-name "Dataproc Fraud Runtime"
```

Grant Dataproc worker role on the project:

```bash
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member "serviceAccount:${SA_EMAIL}" \
  --role "roles/dataproc.worker"
```

Grant GCS permissions on the cloud-native bucket used by Phase 2 scripts:

```bash
gcloud storage buckets add-iam-policy-binding "gs://${GCP_GCS_BUCKET}" \
  --member "serviceAccount:${SA_EMAIL}" \
  --role "roles/storage.objectAdmin"
```

Grant Pub/Sub permissions for streaming input/output:

```bash
gcloud pubsub subscriptions add-iam-policy-binding "$FRAUD_INPUT_SUBSCRIPTION" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.subscriber"

gcloud pubsub topics add-iam-policy-binding "$FRAUD_ALERTS_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher"
```

Grant BigQuery read permissions for daily model refresh:

```bash
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member "serviceAccount:${SA_EMAIL}" \
  --role "roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member "serviceAccount:${SA_EMAIL}" \
  --role "roles/bigquery.readSessionUser"

bq query --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataViewer\` ON SCHEMA \`${GCP_PROJECT_ID}.${FRAUD_BQ_DATASET}\` TO 'serviceAccount:${SA_EMAIL}'"
```

Allow the submitting identity (your user or CI service account) to run Dataproc batches as this runtime service account:

```bash
export SUBMITTER_ACCOUNT="$(gcloud config get-value account)"

gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --member "user:${SUBMITTER_ACCOUNT}" \
  --role "roles/iam.serviceAccountUser"
```

Finally, set the runtime service account variable used by the scripts:

```bash
export GCP_DATAPROC_SERVICE_ACCOUNT="$SA_EMAIL"
```

### 14.4 Streaming-First Runtime Order (Recommended)

Recommended execution order for this repository:

1. Run streaming ingestion/scoring first so fresh scored data is written to GCS.
2. Run hourly batch to build curated/retraining/monitoring outputs from that fresh data.
3. Load hourly curated outputs from GCS into BigQuery raw batch tables.
4. Run daily model refresh after hourly outputs exist.

This avoids pushing stale local data to cloud paths.

### 14.5 Run the Streaming Batch Wrapper First

This wrapper consumes transaction events from your Pub/Sub subscription and writes scored outputs to GCS.

Before submitting the batch, verify that the trained Spark model exists in GCS:

```bash
gsutil ls "${FRAUD_MODEL_PATH}/metadata"
```

If it does not exist yet, generate it locally first (artifacts are gitignored):

```bash
python3 ml/train_fraud_model.py \
  --input data/transaction_log.csv \
  --model-output ml/artifacts/fraud_rf_pipeline
```

Then upload the generated artifact to GCS:

```bash
gsutil -m cp -r ml/artifacts/fraud_rf_pipeline "$FRAUD_MODEL_PATH"
```

If streaming still fails at `PipelineModel.load` with Spark model schema/column errors, the model was likely trained with an incompatible Spark version (for example Spark 4.x locally vs Spark 3.x on Dataproc). Rebuild the model on Dataproc:

```bash
bash scripts/gcp/dataproc/submit_model_train_bootstrap.sh
```

This script will:

- Upload `data/transaction_log.csv` to `gs://${GCP_GCS_BUCKET}/inputs/transaction_log.csv` if missing.
- Train `ml/train_fraud_model.py` on Dataproc runtime.
- Write a Dataproc-compatible model to `FRAUD_MODEL_PATH`.

```bash
bash scripts/gcp/dataproc/submit_streaming_batch.sh
```

It validates Dataproc runtime wiring and produces fresh records under:

- `FRAUD_RAW_PATH`
- `FRAUD_SCORED_PATH` (drives hourly batch)
- `FRAUD_ALERTS_PATH`

### 14.6 Run the Hourly Dataproc Batch

After streaming has produced data in `FRAUD_SILVER_PATH`, run:

```bash
bash scripts/gcp/dataproc/submit_hourly_batch.sh
```

What it does:

- Reads scored transactions from `FRAUD_SILVER_PATH`.
- Joins labels from `FRAUD_LABELS_CSV`.
- Writes curated scored, retraining, and monitoring outputs to `FRAUD_HOURLY_OUTPUT_BASE`.

Optional targeted replay for one UTC hour:

```bash
export FRAUD_TARGET_HOUR_UTC="2026-04-07-13"
bash scripts/gcp/dataproc/submit_hourly_batch.sh
```

### 14.6.1 Load Hourly Batch Outputs to BigQuery

Run this immediately after **14.6** on Dataproc Serverless so curated hourly outputs are available in BigQuery before downstream daily refresh operations.

```bash
bash scripts/gcp/dataproc/submit_hourly_bq_load.sh
```

What it does:

- Submits `batch/load_hourly_batch_to_bigquery.py` as a Dataproc PySpark batch in `gcp-native` mode.
- Reads hourly parquet outputs from `FRAUD_HOURLY_OUTPUT_BASE`.
- Loads `curated_scored`, `retraining_dataset`, and `monitoring_hourly` into `FRAUD_BQ_DATASET`.

Optional controls:

- `FRAUD_BQ_LOAD_WRITE_DISPOSITION=WRITE_APPEND|WRITE_TRUNCATE|WRITE_EMPTY`
- `FRAUD_BQ_LOAD_TABLES="curated_scored retraining_dataset"`
- `FRAUD_BQ_TEMP_BUCKET=<gcs-bucket-for-temp-connector-writes>`

Default tables loaded by this script:

- `curated_scored`
- `retraining_dataset`
- `monitoring_hourly`

### 14.7 Run the Daily Model Refresh Dataproc Batch

Run after **14.6** and **14.6.1** are completed, and after exporting variables from **14.2.2** (at minimum `GCP_PROJECT_ID` and `GCP_GCS_BUCKET`):

```bash
bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
```

If Dataproc submission fails with regional CPU quota errors (for example `CPUS_ALL_REGIONS`), use a valid Serverless Spark minimum shape:

```bash
export GCP_DATAPROC_SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.driver.cores=4,spark.executor.instances=2,spark.executor.cores=4"
bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
```

Dataproc Serverless requires:

- driver cores in `{4, 8, 16}`
- executor cores in `{4, 8, 16}`
- at least 2 executors

So the practical minimum request is 12 vCPUs (4 driver + 2 x 4 executor). If your `CPUS_ALL_REGIONS` quota is 8, this job cannot run in Serverless Spark until you increase quota or submit in a region with available quota >= 12.

What it does:

- Reads retraining data from BigQuery using the Spark BigQuery connector.
- Refreshes the fraud model artifact.
- Writes refreshed `PipelineModel` to `FRAUD_MODEL_OUTPUT` in GCS.

If you need CSV fallback instead of BigQuery:

```bash
export FRAUD_TRAINING_SOURCE="csv"
export FRAUD_SILVER_PATH="gs://${GCP_GCS_BUCKET}/lake/silver/scored_transactions"
export FRAUD_LABELS_CSV="gs://${GCP_GCS_BUCKET}/inputs/transaction_log.csv"
bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
```

### 14.7.1 Optional Bootstrap Path (Only If You Skip Streaming)

Use this only if you need a quick one-time bootstrap and are not running streaming first.

```bash
gsutil cp data/transaction_log.csv "$FRAUD_LABELS_CSV"
gsutil cp -r data/lake/silver/scored_transactions "$FRAUD_SILVER_PATH"
```

If you are using a fresh environment, make sure the model artifact exists in GCS before running streaming or batch wrappers.

### 14.8 Verify Outputs

Check the hourly outputs in GCS:

```bash
gsutil ls "$FRAUD_HOURLY_OUTPUT_BASE/curated_scored"
gsutil ls "$FRAUD_HOURLY_OUTPUT_BASE/retraining_dataset"
gsutil ls "$FRAUD_HOURLY_OUTPUT_BASE/monitoring_hourly"
```

Check the refreshed model artifact:

```bash
gsutil ls "$FRAUD_MODEL_OUTPUT"
```

Inspect Dataproc batch status:

```bash
gcloud dataproc batches list --region "$GCP_REGION"
gcloud dataproc batches describe "$GCP_DATAPROC_HOURLY_BATCH_ID" --region "$GCP_REGION"
gcloud dataproc batches describe "$GCP_DATAPROC_DAILY_BATCH_ID" --region "$GCP_REGION"
```

### 14.9 Common Failure Cases and Fixes

1. `GOOGLE_APPLICATION_CREDENTIALS` is missing in local testing:
  - Set it only for local runs that access `gs://` paths directly.
  - Do not set it on Dataproc Serverless unless you intentionally want to use a JSON key file.
2. BigQuery connector load fails:
  - Confirm the batch script is using a tested `BIGQUERY_CONNECTOR_PACKAGE` value.
  - Confirm the Dataproc service account can read the BigQuery dataset.
3. Daily model refresh fails with `java.util.ServiceConfigurationError ... BigQueryRelationProvider not a subtype`:
  - This is typically a BigQuery connector classpath conflict.
  - First, remove any connector override and use Dataproc's built-in connector:

```bash
unset BIGQUERY_CONNECTOR_PACKAGE
unset GCP_DATAPROC_USE_BIGQUERY_CONNECTOR_OVERRIDE
bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
```

  - If you must override manually for Dataproc 2.2, opt in explicitly and use `_2.12`:

```bash
export BIGQUERY_CONNECTOR_PACKAGE="com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1"
export GCP_DATAPROC_USE_BIGQUERY_CONNECTOR_OVERRIDE="true"
bash scripts/gcp/dataproc/submit_daily_model_refresh.sh
```
4. Hourly job reports no matching rows:
  - Check `FRAUD_TARGET_HOUR_UTC` formatting.
  - Confirm the input parquet contains the requested time window.
5. Streaming wrapper fails immediately:
  - Confirm `FRAUD_INPUT_SUBSCRIPTION` exists and has messages.
  - Confirm Dataproc runtime service account has `roles/pubsub.subscriber` on that subscription.
  - Confirm Dataproc runtime service account has `roles/pubsub.publisher` on `FRAUD_ALERTS_TOPIC` if alert publish is enabled.
6. Streaming fails with `403 ... pubsub.subscriptions.consume`:
  - Re-apply Pub/Sub IAM bindings for the Dataproc runtime service account:

```bash
export SA_EMAIL="$GCP_DATAPROC_SERVICE_ACCOUNT"

gcloud pubsub subscriptions add-iam-policy-binding "$FRAUD_INPUT_SUBSCRIPTION" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.subscriber"

gcloud pubsub topics add-iam-policy-binding "$FRAUD_ALERTS_TOPIC" \
  --project "$GCP_PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher"
```

7. Streaming fails at `PipelineModel.load` with schema/column errors (for example unresolved `treeID`):
  - The model was trained with an incompatible Spark major version.
  - Rebuild model on Dataproc and resubmit streaming:

```bash
bash scripts/gcp/dataproc/submit_model_train_bootstrap.sh
bash scripts/gcp/dataproc/submit_streaming_batch.sh
```

8. Need fast diagnostics for the latest streaming batch failure:

```bash
gcloud dataproc batches list --region "$GCP_REGION" --project "$GCP_PROJECT_ID" --sort-by=~createTime --limit=5
gcloud dataproc batches wait "$GCP_DATAPROC_STREAMING_BATCH_ID" --region "$GCP_REGION" --project "$GCP_PROJECT_ID"
```

9. Streaming fails with `GoogleJsonResponseException: 412 Precondition Failed` when writing to `gs://` outputs:
  - This usually means two writers raced on the same GCS output prefix (for example, two streaming batches writing to the same `FRAUD_RAW_PATH` / `FRAUD_SCORED_PATH` / `FRAUD_ALERTS_PATH`).
  - First, ensure only one streaming Dataproc batch is active:

```bash
gcloud dataproc batches list \
  --region "$GCP_REGION" \
  --project "$GCP_PROJECT_ID" \
  --filter='state=RUNNING' \
  --sort-by=~createTime
```

  - If you find duplicate streaming runs, cancel older ones and rerun only one:

```bash
gcloud dataproc batches delete "<running-streaming-batch-id>" \
  --region "$GCP_REGION" \
  --project "$GCP_PROJECT_ID"

bash scripts/gcp/dataproc/submit_streaming_batch.sh
```

  - If a previous failed run left temporary output state, clear only the temporary directories under the target prefixes (do not wipe full datasets unless you intend a reset):

```bash
gsutil -m rm -r "${FRAUD_RAW_PATH%/}/_temporary/**" || true
gsutil -m rm -r "${FRAUD_SCORED_PATH%/}/_temporary/**" || true
gsutil -m rm -r "${FRAUD_ALERTS_PATH%/}/_temporary/**" || true
```

### 14.10 Cleanup Commands (Optional)

```bash
gcloud dataproc batches list --region "$GCP_REGION"
```

Dataproc batches are managed services and typically do not need manual cleanup after completion. Remove generated GCS outputs only if you want to reset the environment:

```bash
gsutil rm -r "$FRAUD_HOURLY_OUTPUT_BASE"
gsutil rm -r "$FRAUD_MODEL_OUTPUT"
```

---

## 15) Phase 3 Detailed Implementation Guide (Dataform, Based on Current dbt Logic)

This section is an operations-only runbook for Phase 3. Implementation code now lives in the repository under the Dataform project path, while this plan focuses on how to run and validate it.

### 15.1 Implemented Dataform Assets in Repository

The Dataform implementation is created in these locations:

1. Project config:
  - `dataform.json`
  - `package.json`
2. Shared helper:
  - `includes/sql_utils.js`
3. Source declarations:
  - `definitions/sources/curated_scored.sqlx`
  - `definitions/sources/monitoring_hourly.sqlx`
4. Staging:
  - `definitions/staging/stg_curated_scored.sqlx`
  - `definitions/staging/stg_monitoring_hourly.sqlx`
5. Dimensions:
  - `definitions/dimensions/dim_transaction_type.sqlx`
  - `definitions/dimensions/dim_account.sqlx`
  - `definitions/dimensions/dim_time_hour.sqlx`
6. Facts:
  - `definitions/facts/fct_scored_transactions.sqlx`
  - `definitions/facts/fct_fraud_alerts.sqlx`
7. Mart:
  - `definitions/marts/mart_fraud_hourly_kpis.sqlx`
8. Relationship assertions:
  - `definitions/assertions/assert_fct_scored_transactions_fk_transaction_type.sqlx`
  - `definitions/assertions/assert_fct_scored_transactions_fk_origin_account.sqlx`
  - `definitions/assertions/assert_fct_scored_transactions_fk_destination_account.sqlx`

### 15.2 What Logic This Implementation Preserves

The implemented Dataform graph keeps the same transformation behavior as current dbt models:

1. Same input sources from raw BigQuery batch tables.
2. Same staging cast logic and boolean normalization semantics.
3. Same dimension key strategy for transaction type and account.
4. Same transaction-level fact key hashing logic.
5. Same alert-only fact filtering condition.
6. Same hourly KPI mart aggregation and monitoring join behavior.
7. Equivalent data-quality intent through Dataform assertions.

### 15.3 Prerequisites for Fully Managed Cloud Execution

This phase uses managed Dataform service only (no local Dataform CLI runtime in operations).

Before running, ensure:

1. GCP APIs are enabled:
  - dataform.googleapis.com
  - bigquery.googleapis.com
  - secretmanager.googleapis.com
2. BigQuery datasets exist and are accessible:
  - source dataset: fraud_analytics
  - target dataset: fraud_analytics_df
  - assertion dataset: fraud_analytics_assertions
3. Source tables are loaded by Phase 2 flow:
  - curated_scored
  - monitoring_hourly
4. Dataform repository is connected to this Git repository and uses repository root as the Dataform project root.

### 15.4 How to Run Dataform as Fully Managed Service (Without Composer)

Use the Google Cloud Console Dataform UI for repository setup, release configuration, workflow configuration, and workflow invocations.

1. Set environment and enable APIs.

```bash
export GCP_PROJECT_ID="<your-project-id>"
export DATAFORM_REGION="us-central1"
export DATAFORM_REPO_ID="fraud-warehouse"
export DATAFORM_RELEASE_CONFIG_ID="prod"
export DATAFORM_WORKFLOW_CONFIG_ID="fraud-main"

gcloud config set project "$GCP_PROJECT_ID"
gcloud services enable dataform.googleapis.com bigquery.googleapis.com secretmanager.googleapis.com
```

2. Create execution service account (first time only).

```bash
export DATAFORM_EXEC_SA="dataform-exec@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create dataform-exec \
  --project="$GCP_PROJECT_ID" \
  --display-name="Dataform Execution Service Account"
```

3. Grant minimum IAM for execution service account.

```bash
gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:${DATAFORM_EXEC_SA}" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:${DATAFORM_EXEC_SA}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$GCP_PROJECT_ID" \
  --member="serviceAccount:${DATAFORM_EXEC_SA}" \
  --role="roles/bigquery.dataViewer"
```

4. Allow Dataform service agent to impersonate execution service account.

```bash
export PROJECT_NUMBER=$(gcloud projects describe "$GCP_PROJECT_ID" --format='value(projectNumber)')
export DATAFORM_SERVICE_AGENT="service-${PROJECT_NUMBER}@gcp-sa-dataform.iam.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding "$DATAFORM_EXEC_SA" \
  --member="serviceAccount:${DATAFORM_SERVICE_AGENT}" \
  --role="roles/iam.serviceAccountUser"
```

5. Create Dataform repository (first time only).

Use Google Cloud Console UI:

- Open Dataform in Google Cloud Console.
- Go to `Repositories` -> `Create repository`.
- Set repository ID to `${DATAFORM_REPO_ID}` and region to `${DATAFORM_REGION}`.
- Select execution service account `${DATAFORM_EXEC_SA}` during repository setup.
- Dataform UI will suggest required IAM roles/bindings for the selected service account. Apply the suggested roles in UI.
- Click `Create`.

6. Configure repository settings in Dataform UI (one time):
  - Connect Git remote and branch.
  - Use repository root as the project root.
  - Confirm Dataform workflow settings files are detected at repository root.
  - If Git connection fails with `secretmanager.versions.access`, grant `roles/secretmanager.secretAccessor` on the Git credential secret to the default Dataform service agent or the selected execution service account.

7. Create release config in Dataform UI for managed compilation/execution.
  - Open the repository in Google Cloud Console.
  - Go to `Release configurations` and create a new release config.
  - Set the git commitish to `main`.
  - Set the default project/database to `GCP_PROJECT_ID`.
  - Set the default schema to `fraud_analytics_df`.
  - Set the assertion schema to `fraud_analytics_assertions`.
  - Set the execution service account to `DATAFORM_EXEC_SA`.

8. Create workflow config in Dataform UI for managed runs.
  - Go to `Workflow configurations` and create a new workflow config.
  - Attach it to the release config created in the previous step.
  - Select the actions or tags for the Wave A subset first.

9. Run Wave A from the Dataform UI.
  - Open the workflow config.
  - Click `Run` to start a workflow invocation.
  - Use the invocation details page to inspect logs and status.

10. Check workflow run status in the Dataform UI.
  - Open `Workflow invocations` to review recent runs.
  - Confirm the run reaches `Succeeded` before promoting the next wave.

Operational recommendation:

1. Use a dedicated Wave A workflow config first.
2. Add full-model workflow config after Wave A parity passes.
3. Keep manual cloud invocation in this phase; Composer trigger is handled in later phase.

### 15.5 How to Validate Parity Against Existing dbt Outputs

Use side-by-side comparison between:

1. Existing dbt dataset output.
2. New Dataform dataset output.

For each wave, validate:

1. Row-count parity per equivalent model.
2. Key KPI parity on hourly fraud metrics.
3. Required-column presence and nullability contract.

Command-line parity checks (run from Cloud Shell or any environment with bq access):

1. Confirm both datasets exist.

```bash
bq ls "${GCP_PROJECT_ID}:fraud_analytics"
bq ls "${GCP_PROJECT_ID}:fraud_analytics_df"
```

2. Row-count parity for scored fact.

```bash
bq query --use_legacy_sql=false "
select
  'fct_scored_transactions' as model_name,
  (select count(*) from \`${GCP_PROJECT_ID}.fraud_analytics.fct_scored_transactions\`) as dbt_count,
  (select count(*) from \`${GCP_PROJECT_ID}.fraud_analytics_df.fct_scored_transactions\`) as dataform_count,
  safe_divide(
    abs(
      (select count(*) from \`${GCP_PROJECT_ID}.fraud_analytics_df.fct_scored_transactions\`) -
      (select count(*) from \`${GCP_PROJECT_ID}.fraud_analytics.fct_scored_transactions\`)
    ),
    nullif((select count(*) from \`${GCP_PROJECT_ID}.fraud_analytics.fct_scored_transactions\`), 0)
  ) as relative_delta
"
```

3. KPI parity for hourly mart.

```bash
bq query --use_legacy_sql=false "
with dbt_kpi as (
  select event_hour_utc, transaction_type, observed_fraud_rate
  from \`${GCP_PROJECT_ID}.fraud_analytics.mart_fraud_hourly_kpis\`
),
df_kpi as (
  select event_hour_utc, transaction_type, observed_fraud_rate
  from \`${GCP_PROJECT_ID}.fraud_analytics_df.mart_fraud_hourly_kpis\`
)
select
  coalesce(d.event_hour_utc, f.event_hour_utc) as event_hour_utc,
  coalesce(d.transaction_type, f.transaction_type) as transaction_type,
  d.observed_fraud_rate as dbt_rate,
  f.observed_fraud_rate as dataform_rate,
  abs(coalesce(f.observed_fraud_rate, 0) - coalesce(d.observed_fraud_rate, 0)) as abs_delta
from dbt_kpi d
full outer join df_kpi f
  on d.event_hour_utc = f.event_hour_utc
 and d.transaction_type = f.transaction_type
order by event_hour_utc desc, transaction_type
"
```

4. Check required columns exist in Dataform outputs.

```bash
bq show --schema --format=prettyjson "${GCP_PROJECT_ID}:fraud_analytics_df.fct_scored_transactions"
bq show --schema --format=prettyjson "${GCP_PROJECT_ID}:fraud_analytics_df.mart_fraud_hourly_kpis"
```

Accept migration wave only if thresholds remain within plan limits:

1. Row-count delta per hourly table at or below 0.5%.
2. Fraud-rate KPI delta at or below 1% relative difference.
3. Zero missing required columns.

### 15.6 Runbook for CI and Scheduled Execution

For CI in this phase (without Composer):

1. Trigger Dataform managed workflow invocation by API/CLI after merge to main.
2. Poll invocation state until succeeded or failed.
3. Block release if invocation fails or if parity checks fail.

Example manual execution pattern:

1. Open the Dataform repository in Google Cloud Console.
2. Open the target workflow config.
3. Click `Run` to create a workflow invocation.
4. Review invocation logs and status in the UI.
5. Gate release promotion on a successful run and parity checks.

Scheduled execution in this phase:

1. Use Dataform workflow config scheduling in Dataform service, or
2. Use Cloud Scheduler + Cloud Run/Cloud Function wrapper to call Dataform API.
3. Keep Composer integration deferred to Phase 4.

### 15.7 Definition of Done for Phase 3

Phase 3 is complete only when all are true:

1. Dataform outputs are parity-validated for required production model scope.
2. Assertions cover key non-null, uniqueness, and relationship checks.
3. Wave-by-wave run evidence is recorded in operations documentation.
4. Dataform workflow configs are runnable in fully managed cloud mode without local execution dependency.

---

## 16) Phase 4 Detailed Implementation Guide (Composer + Dataform Invocation)

This section describes the implemented Phase 4 orchestration migration and how to run it in Cloud Composer.

### 16.1 Implemented Phase 4 Assets in Repository

Phase 4 implementation is now reflected in these repository paths:

1. Composer-ready DAGs:
  - `airflow/dags/fraud_hourly_orchestration.py`
  - `airflow/dags/fraud_daily_model_refresh.py`
2. Composer helper commands:
  - `scripts/gcp/composer/sync_dags_to_composer.sh`
  - `scripts/gcp/composer/set_composer_airflow_variables.sh`
3. Updated environment templates/docs for orchestration:
  - `airflow/.env.example`
  - `scripts/airflow/write_airflow_env.sh`
  - `airflow/README.md`

### 16.2 What Changed in Orchestration Behavior

The migration keeps the original orchestration intent but removes local Docker/dbt assumptions from the cloud runtime path.

1. Hourly DAG now executes:
  - Dataproc Serverless batch for hourly processing.
  - Dataproc Serverless batch for hourly BigQuery load.
  - Dataform workflow invocation for transforms.
  - Dataform workflow invocation for assertions.
2. Daily DAG now executes:
  - Dataproc Serverless daily model refresh batch.
3. Runtime configuration is sourced from Airflow Variables (Composer) with env var fallback.
4. dbt CLI tasks are replaced by Dataform workflow invocation tasks.

### 16.3 Prerequisites Before Deploying Composer DAGs

1. Composer environment exists and is healthy.
2. Dataform repository, release config, and workflow configs from Phase 3 are already created.
3. Dataproc runtime service account and IAM from Phase 2 are already configured.
4. GCS bucket exists for:
  - code artifact staging (`gs://<bucket>/code/...`)
  - lake paths (`gs://<bucket>/lake/...`)
  - model artifacts (`gs://<bucket>/ml/artifacts/...`)
5. Required APIs enabled:
  - `composer.googleapis.com`
  - `dataproc.googleapis.com`
  - `dataform.googleapis.com`
  - `bigquery.googleapis.com`
  - `storage.googleapis.com`

Enable APIs if needed:

```bash
gcloud services enable composer.googleapis.com dataproc.googleapis.com dataform.googleapis.com bigquery.googleapis.com storage.googleapis.com
```

### 16.4 Set Deployment Environment Variables

### 16.4.1 How to Generate Values Before Exporting

Use this discovery flow first, then run the export block in **16.4**.

1. Authenticate and choose project:

```bash
gcloud auth login
gcloud projects list
gcloud config set project "<your-project-id>"
```

2. Discover Composer environment and region:

```bash
gcloud composer environments list --project "$(gcloud config get-value project)" --locations <your-location>
```

From the output, select:

- `COMPOSER_ENV_NAME`
- `COMPOSER_REGION`

If the list is empty, create a dedicated Composer runtime service account first, then create Composer environment in UI.

2.1 Create dedicated Composer runtime service account (recommended):

```bash
export GCP_PROJECT_ID="$(gcloud config get-value project)"
export COMPOSER_SA_NAME="composer-fraud-runtime"
export COMPOSER_RUNTIME_SA="${COMPOSER_SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

gcloud iam service-accounts create "${COMPOSER_SA_NAME}" \
  --project "${GCP_PROJECT_ID}" \
  --display-name "Composer Fraud Runtime"
```

Grant baseline roles to the Composer runtime service account:

```bash
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member "serviceAccount:${COMPOSER_RUNTIME_SA}" \
  --role "roles/composer.worker"

gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member "serviceAccount:${COMPOSER_RUNTIME_SA}" \
  --role "roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member "serviceAccount:${COMPOSER_RUNTIME_SA}" \
  --role "roles/logging.logWriter"

gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
  --member "serviceAccount:${COMPOSER_RUNTIME_SA}" \
  --role "roles/monitoring.metricWriter"
```

2.2 Create Composer environment in Google Cloud Console UI:

1. Open `Composer` in Cloud Console.
2. Click `Create environment`.
3. Choose `Composer 2`.
4. Select region (recommended: `us-central1` unless your platform standards require another region).
5. Set environment name (example: `fraud-orchestrator`).
6. In service account selection, choose `COMPOSER_RUNTIME_SA` created in step 2.1.
7. Select VPC and subnet aligned with your data platform networking.
8. Choose environment size (start with Small for staged validation).
9. Click `Create` and wait until status is healthy/running.

Then set values explicitly:

```bash
export COMPOSER_ENV_NAME="fraud-orchestrator"
export COMPOSER_REGION="us-central1"
```

3. Discover or create the GCS bucket used for lake and code artifacts:

```bash
gcloud storage buckets list --project "$(gcloud config get-value project)"
```

If needed, create one:

```bash
gcloud storage buckets create "gs://<your-lake-and-code-bucket>" \
  --project "$(gcloud config get-value project)" \
  --location "<your-region>"
```

4. Generate `GCP_DATAPROC_SERVICE_ACCOUNT` value:

```bash
export SA_NAME="dataproc-fraud-runtime"
export GCP_PROJECT_ID="$(gcloud config get-value project)"
export GCP_DATAPROC_SERVICE_ACCOUNT="${SA_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
```

If not created yet, create it:

```bash
gcloud iam service-accounts create "${SA_NAME}" \
  --project "${GCP_PROJECT_ID}" \
  --display-name "Dataproc Fraud Runtime"
```

5. Discover subnet for `GCP_DATAPROC_SUBNET`:

```bash
gcloud compute networks subnets list \
  --project "${GCP_PROJECT_ID}" \
  --regions "<your-region>"
```

Then build value in this format:

```bash
projects/${GCP_PROJECT_ID}/regions/<your-region>/subnetworks/<your-subnet-name>
```

Set these once per shell session before deploying:

```bash
export GCP_PROJECT_ID="<your-project-id>"
export GCP_REGION="us-central1"
export COMPOSER_PROJECT_ID="${GCP_PROJECT_ID}"
export COMPOSER_REGION="${GCP_REGION}"
export COMPOSER_ENV_NAME="<your-composer-environment-name>"

export GCP_GCS_BUCKET="<your-lake-and-code-bucket>"
export GCP_DATAPROC_DEPS_BUCKET="${GCP_GCS_BUCKET}"
export GCP_DATAPROC_SERVICE_ACCOUNT="dataproc-fraud-runtime@${GCP_PROJECT_ID}.iam.gserviceaccount.com"
export GCP_DATAPROC_SUBNET="projects/${GCP_PROJECT_ID}/regions/${GCP_REGION}/subnetworks/<subnet-name>"
export GCP_DATAPROC_SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.driver.cores=4,spark.executor.instances=2,spark.executor.cores=4"

export FRAUD_GCP_PROJECT_ID="${GCP_PROJECT_ID}"
export FRAUD_SILVER_PATH="gs://${GCP_GCS_BUCKET}/lake/silver/scored_transactions"
export FRAUD_LABELS_CSV="gs://${GCP_GCS_BUCKET}/inputs/transaction_log.csv"
export FRAUD_HOURLY_OUTPUT_BASE="gs://${GCP_GCS_BUCKET}/lake/gold/hourly_batch"
export FRAUD_BQ_DATASET="fraud_analytics"
export FRAUD_RETRAINING_TABLE="retraining_dataset"
export FRAUD_BQ_TEMP_BUCKET="${GCP_GCS_BUCKET}"
export FRAUD_MODEL_OUTPUT="gs://${GCP_GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline"

export DATAFORM_REGION="${GCP_REGION}"
export DATAFORM_REPOSITORY_ID="fraud-warehouse"
export DATAFORM_RUN_WORKFLOW_CONFIG_ID="fraud-main"
export DATAFORM_ASSERT_WORKFLOW_CONFIG_ID="fraud-assertions"

export FRAUD_HOURLY_BATCH_PY_URI="gs://${GCP_GCS_BUCKET}/code/batch/hourly_batch_processing.py"
export FRAUD_HOURLY_BQ_LOAD_PY_URI="gs://${GCP_GCS_BUCKET}/code/batch/load_hourly_batch_to_bigquery.py"
export FRAUD_DAILY_MODEL_REFRESH_PY_URI="gs://${GCP_GCS_BUCKET}/code/batch/daily_model_refresh.py"
```

### 16.5 Upload Batch Entry Scripts to GCS Code Path

The Composer DAGs submit Dataproc jobs from GCS URIs. Upload the Python entrypoints:

```bash
gsutil cp batch/hourly_batch_processing.py "${FRAUD_HOURLY_BATCH_PY_URI}"
gsutil cp batch/load_hourly_batch_to_bigquery.py "${FRAUD_HOURLY_BQ_LOAD_PY_URI}"
gsutil cp batch/daily_model_refresh.py "${FRAUD_DAILY_MODEL_REFRESH_PY_URI}"
```

Optional validation:

```bash
gsutil ls "${FRAUD_HOURLY_BATCH_PY_URI}"
gsutil ls "${FRAUD_HOURLY_BQ_LOAD_PY_URI}"
gsutil ls "${FRAUD_DAILY_MODEL_REFRESH_PY_URI}"
```

### 16.6 Set Composer Airflow Variables

Run the helper command:

```bash
bash scripts/gcp/composer/set_composer_airflow_variables.sh
```

Manual fallback (single variable example):

```bash
gcloud composer environments run "${COMPOSER_ENV_NAME}" \
  --location "${COMPOSER_REGION}" \
  --project "${COMPOSER_PROJECT_ID}" \
  variables set -- FRAUD_GCP_PROJECT_ID "${FRAUD_GCP_PROJECT_ID}"
```

### 16.7 Deploy DAG Files to Composer

Run the helper command:

```bash
bash scripts/gcp/composer/sync_dags_to_composer.sh
```

Manual fallback:

1. Get Composer DAG bucket path:

```bash
export DAG_GCS_PREFIX="$(gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
  --location "${COMPOSER_REGION}" \
  --project "${COMPOSER_PROJECT_ID}" \
  --format='value(config.dagGcsPrefix)')"

echo "${DAG_GCS_PREFIX}"
```

2. Upload DAG files:

```bash
gsutil cp airflow/dags/fraud_hourly_orchestration.py "${DAG_GCS_PREFIX}/"
gsutil cp airflow/dags/fraud_daily_model_refresh.py "${DAG_GCS_PREFIX}/"
```

Do not keep angle-bracket placeholders in these commands. `DAG_GCS_PREFIX` must be the real value returned from `config.dagGcsPrefix`.

### 16.8 Run and Validate in Composer UI

Use Cloud Console UI:

1. Open `Composer` -> your environment -> `Open Airflow UI`.
2. Confirm DAGs are visible:
  - `fraud_hourly_batch_and_warehouse`
  - `fraud_daily_model_refresh`
3. Unpause both DAGs.
4. Trigger manual validation runs from UI:
  - Hourly DAG: trigger now and monitor each task state.
  - Daily DAG: trigger now and monitor each task state.
5. Confirm all tasks reach `success`.

### 16.9 Run and Validate from Command Line

Trigger hourly DAG:

```bash
gcloud composer environments run "${COMPOSER_ENV_NAME}" \
  --location "${COMPOSER_REGION}" \
  --project "${COMPOSER_PROJECT_ID}" \
  dags trigger -- fraud_hourly_batch_and_warehouse
```

Trigger daily DAG:

```bash
gcloud composer environments run "${COMPOSER_ENV_NAME}" \
  --location "${COMPOSER_REGION}" \
  --project "${COMPOSER_PROJECT_ID}" \
  dags trigger -- fraud_daily_model_refresh
```

List DAG runs:

```bash
gcloud composer environments run "${COMPOSER_ENV_NAME}" \
  --location "${COMPOSER_REGION}" \
  --project "${COMPOSER_PROJECT_ID}" \
  dags list-runs -- --dag-id fraud_hourly_batch_and_warehouse

gcloud composer environments run "${COMPOSER_ENV_NAME}" \
  --location "${COMPOSER_REGION}" \
  --project "${COMPOSER_PROJECT_ID}" \
  dags list-runs -- --dag-id fraud_daily_model_refresh
```

### 16.10 Phase 4 Troubleshooting Checklist

1. DAG import errors in Composer UI:
  - Verify Google provider compatibility with Composer image.
  - Verify Airflow Variables are set and not empty.
2. Dataproc task fails quickly:
  - Confirm `FRAUD_*_PY_URI` files exist in GCS.
  - Confirm runtime service account has Dataproc, GCS, and BigQuery permissions.
3. Dataform task fails:
  - Confirm repository id, region, and workflow config ids are correct.
  - Verify Dataform workflow config runs manually in Dataform UI.
4. Quota or region errors:
  - Tune `GCP_DATAPROC_SPARK_PROPERTIES` to valid Serverless shapes.
  - Verify regional quota supports the configured shape.

### 16.11 Exit Criteria for Phase 4 Completion

Phase 4 is complete when:

1. Both Composer DAGs are deployed and schedulable.
2. Hourly run succeeds end-to-end:
  - Dataproc hourly batch
  - Dataproc BigQuery load
  - Dataform transform invocation
  - Dataform assertion invocation
3. Daily model refresh run succeeds end-to-end in Composer.
4. Run evidence (run ids, timestamps, success state) is recorded in operations notes.
