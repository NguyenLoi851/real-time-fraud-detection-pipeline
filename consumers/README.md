# Alert Consumers (Kafka `fraud_alerts`)

These consumers let you scale notifications independently from Spark streaming.

## Available consumers

- `consumers/alert_email_consumer.py`: sends email per alert event.

Each consumer uses a different default Kafka consumer group so they can run in parallel and all receive alerts.

## Prerequisites

```bash
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## Recommended config workflow (avoid repeated exports)

1. Create your local config file once:

```bash
cp consumers/.env.example consumers/.env
```

2. Edit `consumers/.env` with your real values.

3. Run consumers normally. They auto-load values from `consumers/.env`.

`consumers/.env` is ignored by git (safe for local secrets). CLI flags still override `.env` values.

## 1) Email sender setup (your own email)

For most personal email providers, use an **App Password** (not your normal login password).

### A) Generate app password

- **Gmail / Google Workspace**
  1. Enable 2-Step Verification in your Google account.
  2. Open Google Account → Security → App passwords.
  3. Create an app password for Mail and copy the generated 16-character password.

- **Outlook / Microsoft 365**
  1. Enable 2-step verification for your Microsoft account.
  2. Open Security settings → Advanced security options → App passwords.
  3. Create a new app password and copy it.

### B) Configure SMTP in `consumers/.env`

Set your sender details in `consumers/.env` (created from `consumers/.env.example`).

Provider defaults:

- Gmail: `ALERT_SMTP_HOST=smtp.gmail.com`, `ALERT_SMTP_PORT=587`
- Outlook: `ALERT_SMTP_HOST=smtp.office365.com`, `ALERT_SMTP_PORT=587`

## 2) Email consumer

```bash
python3 consumers/alert_email_consumer.py \
  --bootstrap-servers "${ALERT_BOOTSTRAP_SERVERS:-localhost:9092}" \
  --topic "${ALERT_TOPIC:-fraud_alerts}" \
  --group-id fraud-alerts-email-consumer \
  --email-use-tls
```

If needed, CLI flags still override values from environment variables.

## 3) Quick test without running full streaming flow

You can publish dummy alert messages directly to Kafka and verify consumers immediately.

1. Ensure Kafka is up and topic exists:

```bash
./scripts/kafka/kafka_up.sh
./scripts/kafka/kafka_topic_create.sh fraud_alerts
```

2. Start a consumer (example: debug consumer):

```bash
python3 simulator/kafka/kafka_basic_consumer.py \
  --bootstrap-servers "${ALERT_BOOTSTRAP_SERVERS:-localhost:9092}" \
  --topic "${ALERT_TOPIC:-fraud_alerts}" \
  --group-id fraud-alerts-debug-consumer \
  --from-beginning
```

3. Publish dump/test messages into `fraud_alerts`:

```bash
python3 consumers/publish_test_alert.py \
  --bootstrap-servers "${ALERT_BOOTSTRAP_SERVERS:-localhost:9092}" \
  --topic "${ALERT_TOPIC:-fraud_alerts}" \
  --count 5
```

4. Run email consumer in another terminal to verify it processes alert events.

## Notes

- Use `--from-beginning` to replay old messages.
- Use `--max-messages N` for quick tests.
- Keep credentials/webhooks in environment variables or a secret manager in production.
- If your provider blocks login, confirm app-password support and SMTP access are enabled.
- Consumers process all messages in `fraud_alerts` (no score filter), assuming Spark already filtered alerts before publishing.
