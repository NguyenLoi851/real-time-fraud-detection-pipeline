# Pub/Sub Transaction Publisher

## Purpose

Publish transaction events from CSV to a Pub/Sub topic for cloud-native ingestion.

## Prerequisites

Complete shared setup first: [../../docs/prerequisites.md](../../docs/prerequisites.md)

## Create Topic

```bash
gcloud pubsub topics create transactions_raw
```

## Run Publisher

```bash
python3 simulator/pubsub/pubsub_csv_publisher.py \
  --input data/transaction_log.csv \
  --project-id "$GCP_PROJECT_ID" \
  --topic transactions_raw \
  --interval-min 0.3 \
  --interval-max 1.0 \
  --max-events 50
```

## Verify Messages

Use a temporary pull subscription:

```bash
gcloud pubsub subscriptions create transactions-raw-debug-sub --topic=transactions_raw
gcloud pubsub subscriptions pull transactions-raw-debug-sub --limit=5 --auto-ack
```

For end-to-end execution order, see [../../docs/cloud-migration-plan.md](../../docs/cloud-migration-plan.md).
