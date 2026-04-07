# Simulator Module

## Purpose

Generate transaction events and publish them for streaming ingestion.

## Module Index

- CSV event simulation: [csv/README.md](csv/README.md)
- Kafka producer and validation consumer: [kafka/README.md](kafka/README.md)
- Pub/Sub transaction publisher (cloud path): [pubsub/README.md](pubsub/README.md)

## Recommended Flow

1. Start Kafka stack from [kafka/README.md](kafka/README.md).
2. Run producer to publish `transactions_raw`.
3. Validate topic consumption with basic consumer.

For full local execution, see [../docs/runbook-local.md](../docs/runbook-local.md).
