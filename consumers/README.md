# Alert Consumers

## Purpose

Consume Kafka `fraud_alerts` events and send notifications independently from Spark.

## Available Consumer

- `consumers/alert_email_consumer.py`
- `consumers/alert_pubsub_consumer.py` (Local Pub/Sub pull worker)

## Prerequisites

Complete shared setup first: [../docs/prerequisites.md](../docs/prerequisites.md)

## Configuration

```bash
cp consumers/.env.example consumers/.env
```

Set SMTP values in `consumers/.env`.

Common defaults:

- Gmail: `ALERT_SMTP_HOST=smtp.gmail.com`, `ALERT_SMTP_PORT=587`
- Outlook: `ALERT_SMTP_HOST=smtp.office365.com`, `ALERT_SMTP_PORT=587`

## Run Email Consumer

```bash
python3 consumers/alert_email_consumer.py \
  --bootstrap-servers "${ALERT_BOOTSTRAP_SERVERS:-localhost:9092}" \
  --topic "${ALERT_TOPIC:-fraud_alerts}" \
  --group-id fraud-alerts-email-consumer \
  --email-use-tls
```

## Quick Test

```bash
./scripts/kafka/kafka_topic_create.sh fraud_alerts
python3 consumers/publish_test_alert.py --topic fraud_alerts --count 5
```

## Run Pub/Sub Consumer (Local)

Run the Pub/Sub consumer in pull mode (recommended):

```bash
python3 consumers/alert_pubsub_consumer.py \
  --pubsub-project-id "$GCP_PROJECT_ID" \
  --pubsub-subscription "$ALERT_PULL_SUBSCRIPTION" \
  --delivery-mode email \
  --email-use-tls
```

Create the pull subscription once if needed:

```bash
gcloud pubsub subscriptions create "$ALERT_PULL_SUBSCRIPTION" \
  --topic="$PUBSUB_FRAUD_ALERTS_TOPIC"
```

## Troubleshooting

See shared operations guide: [../docs/operations.md](../docs/operations.md)
