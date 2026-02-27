# Kafka Producer and Consumer

This module sends transaction events to Kafka and provides a basic consumer for validation.

## Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

For every new terminal:

```bash
source .venv/bin/activate
```

## Start Kafka

```bash
chmod +x scripts/kafka/kafka_up.sh scripts/kafka/kafka_down.sh scripts/kafka/kafka_topics.sh scripts/kafka/kafka_topic_create.sh
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
