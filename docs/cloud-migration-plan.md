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
| Warehouse transform | dbt BigQuery | Dataform (BigQuery-native) | Run both in transition; eventually pick one primary |
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
   - `v2.2-dataform` (if adopted)

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
  - `dbt` remains transitional source of truth initially
  - add `dataform/` when conversion starts

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

## Phase 3: Orchestration Migration (Docker Airflow -> Composer) (3-5 days)

Deliverables:

- Port current DAGs to Composer-compatible deployment:
  - `airflow/dags/fraud_hourly_orchestration.py`
  - `airflow/dags/fraud_daily_model_refresh.py`
- Replace local path assumptions with GCS/Composer env variables.
- Add Composer deployment script and operations runbook.

Implementation notes:

- Keep task order identical to reduce regression risk.
- First run Composer DAGs against existing dbt path.

Exit criteria:

- Scheduled runs succeed in Composer for hourly and daily pipelines.

## Phase 4: Warehouse Evolution (dbt + Dataform transition) (5-10 days)

Deliverables:

- Keep dbt as default transform engine initially.
- Create equivalent Dataform project for a subset:
  - dimensions
  - one fact
  - one mart
- Compare outputs (row count + key metric parity).

Decision gate:

- Option A: keep dbt as primary and stop Dataform expansion.
- Option B: migrate fully to Dataform and keep dbt as fallback until stable.

Exit criteria:

- Documented recommendation with benchmark and ops trade-offs.

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
- `TRANSFORM_BACKEND=dbt|dataform`

Guideline:

- Runtime selection must happen in wrapper scripts and DAG params.

---

## 7) Testing and Parity Plan

Minimum validation suite per phase:

1. Contract tests (schema + required columns).
2. Data parity checks for one fixed hourly window.
3. DAG task success checks with expected artifacts present.
4. dbt/Dataform model quality checks (or equivalent assertions).

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
3. Dual-transform maintenance overhead (dbt + Dataform).
   Mitigation: time-box coexistence and define decision gate in Phase 4.
4. Cost increase from always-on managed services.
   Mitigation: start with scheduled/serverless jobs and budget alerts.

---

## 9) First Build Backlog (Implementation Order)

1. Add GCP config profile and runtime variables.
2. Add Pub/Sub producer and alert consumer path.
3. Add Dataproc submission scripts for hourly/daily jobs.
4. Add Composer environment + DAG deployment path.
5. Add Dataform skeleton and first model parity checks.
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
