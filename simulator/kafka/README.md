# Kafka Producer and Consumer

## Purpose

Publish transaction events to Kafka and validate topic flow with a basic consumer.

## Prerequisites

Complete shared setup first: [../../docs/prerequisites.md](../../docs/prerequisites.md)

## Start Kafka

```bash
./scripts/kafka/kafka_up.sh
```

Verify:

```bash
docker ps --filter name=fraud-kafka
docker logs -f fraud-kafka
```

Compose file location:

`simulator/kafka/docker-compose.kafka.yml`

## Topic Commands

```bash
./scripts/kafka/kafka_topic_create.sh transactions_raw
./scripts/kafka/kafka_topics.sh --list
./scripts/kafka/kafka_topics.sh --describe --topic transactions_raw
./scripts/kafka/kafka_topic_count.sh transactions_raw
```

## Run Producer

```bash
python3 simulator/kafka/kafka_csv_producer.py \
	--input data/transaction_log.csv \
	--bootstrap-servers localhost:9092 \
	--topic transactions_raw \
	--interval-min 0.3 \
	--interval-max 1.0 \
	--max-events 50 \
	--log-row-details
```

## Run Basic Consumer

Open another terminal:

```bash
python3 simulator/kafka/kafka_basic_consumer.py \
	--bootstrap-servers localhost:9092 \
	--topic transactions_raw \
	--group-id fraud-debug-consumer \
	--from-beginning \
	--max-messages 50
```

## Stop Kafka

```bash
./scripts/kafka/kafka_down.sh
```

## Note

`simulator/csv/realtime_csv_simulator.py` is optional when using Kafka flow.

For end-to-end execution order, see [../../docs/runbook-local.md](../../docs/runbook-local.md).
