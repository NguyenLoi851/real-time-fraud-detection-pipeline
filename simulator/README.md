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
