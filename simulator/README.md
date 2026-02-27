# Simulator Module

This folder contains the real-time CSV simulator used in the current phase of the project.

## What It Does

- Reads transactions row-by-row from a source CSV.
- Waits with a random delay between events.
- Writes emitted events to an output CSV file.
- Excludes `isFraud` and `isFlaggedFraud` by default (to match real-time inference).

## Quick Start

Run the simulator to emit one transaction at a time and write to an output file:

```bash
python3 simulator/realtime_csv_simulator.py \
	--input data/transaction_log.csv \
	--output data/realtime_transactions.csv \
	--interval-min 0.3 \
	--interval-max 2.0 \
	--max-events 100 \
	--overwrite
```

## Run With Docker (No Local Python Needed)

Use one script from project root:

```bash
./scripts/run_simulator_docker.sh
```

That command will:

- build the Docker image
- run the simulator in detached mode with default settings
- write output to `data/realtime_transactions.csv`
- print detailed row logs (`--log-row-details`)

If you want custom values, pass simulator arguments directly:

```bash
./scripts/run_simulator_docker.sh \
	--input data/transaction_log.csv \
	--output data/realtime_transactions.csv \
	--interval-min 0.3 \
	--interval-max 2.0 \
	--max-events 100 \
	--log-row-details \
	--overwrite
```

### Logs and Container Management

Use custom container name:

```bash
./scripts/run_simulator_docker.sh --name fraud-simulator-dev
```

See logs:

```bash
docker logs -f fraud-simulator-run
```

Stop container:

```bash
docker stop fraud-simulator-run
```

Notes:
- Keep running commands from project root.
- Make the script executable once if needed: `chmod +x scripts/run_simulator_docker.sh`.

## Options

- `--input`: source CSV path
- `--output`: output CSV path
- `--interval-min`: minimum delay in seconds between events
- `--interval-max`: maximum delay in seconds between events
- `--max-events`: max number of rows to emit (`0` means no limit)
- `--include-labels`: include `isFraud` and `isFlaggedFraud` (debug only)
- `--overwrite`: overwrite output file instead of append
- `--log-row-details`: print key row values for each emitted event

## Fixed-Like Delay

Set min and max to the same value:

```bash
python3 simulator/realtime_csv_simulator.py \
	--input data/transaction_log.csv \
	--output data/realtime_transactions.csv \
	--interval-min 1.0 \
	--interval-max 1.0 \
	--max-events 100 \
	--overwrite
```

## Current Handoff

The simulator output file (`data/realtime_transactions.csv`) is the handoff point before sending events to Kafka in the next phase.

## Kafka Integration (Docker + Python)

This section matches the project architecture in the root README:

- Topic for raw transactions: `transactions_raw`
- Kafka runs in Docker
- Python producer sends row-by-row events to Kafka

### 0) Set up Python venv (recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

When opening a new terminal, run:

```bash
source .venv/bin/activate
```

If you prefer not to activate, use `.venv/bin/python` directly in commands.

Note:
- `realtime_csv_simulator.py` is optional for Kafka phase.
- For Kafka streaming, use `kafka_csv_producer.py` directly.

### 1) Install Python dependency

```bash
python3 -m pip install -r requirements.txt
```

### 2) Start Kafka broker (Docker)

```bash
chmod +x scripts/kafka/kafka_up.sh scripts/kafka/kafka_down.sh scripts/kafka/kafka_topics.sh scripts/kafka/kafka_topic_create.sh
./scripts/kafka/kafka_up.sh
```

What this does:

- Starts Kafka from `docker/docker-compose.kafka.yml` in detached mode
- Broker port is `localhost:9092`

### 3) Verify broker is running

```bash
docker ps --filter name=fraud-kafka
docker logs -f fraud-kafka
```

### 4) Create and verify topic

Create topic:

```bash
./scripts/kafka/kafka_topic_create.sh transactions_raw
```

List topics:

```bash
./scripts/kafka/kafka_topics.sh --list
```

Describe topic:

```bash
./scripts/kafka/kafka_topics.sh --describe --topic transactions_raw
```

### 5) Run Python producer (CSV -> Kafka)

```bash
python3 simulator/kafka_csv_producer.py \
	--input data/transaction_log.csv \
	--bootstrap-servers localhost:9092 \
	--topic transactions_raw \
	--interval-min 0.3 \
	--interval-max 1.0 \
	--max-events 50 \
	--log-row-details
```

What this producer code does:

- Reads CSV row-by-row
- Excludes `isFraud` and `isFlaggedFraud` by default
- Adds `event_emitted_at_utc`
- Sends JSON messages to Kafka topic
- Prints topic/partition/offset for each message

### 6) Run basic consumer to test messages

Open another terminal and run:

```bash
python3 simulator/kafka_basic_consumer.py \
	--bootstrap-servers localhost:9092 \
	--topic transactions_raw \
	--group-id fraud-debug-consumer \
	--from-beginning \
	--max-messages 50
```

What this consumer code does:

- Subscribes to the topic
- Reads messages and prints partition/offset/value
- Helps verify broker/topic/data pipeline is working

### 7) Stop Kafka

```bash
./scripts/kafka/kafka_down.sh
```
