# CSV Real-Time Simulator

This module simulates real-time transaction events from CSV to CSV output.

## What It Does

- Reads transactions row-by-row from a source CSV.
- Waits with a random delay between events.
- Writes emitted events to an output CSV file.
- Excludes `isFraud` and `isFlaggedFraud` by default.

## Quick Start (Python)

```bash
python3 simulator/csv/realtime_csv_simulator.py \
	--input data/transaction_log.csv \
	--output data/realtime_transactions.csv \
	--interval-min 0.3 \
	--interval-max 2.0 \
	--max-events 100 \
	--overwrite
```

## Run With Docker

```bash
chmod +x scripts/simulator/run_simulator_docker.sh
./scripts/simulator/run_simulator_docker.sh
```

Custom run:

```bash
./scripts/simulator/run_simulator_docker.sh \
	--input data/transaction_log.csv \
	--output data/realtime_transactions.csv \
	--interval-min 0.3 \
	--interval-max 2.0 \
	--max-events 100 \
	--log-row-details \
	--overwrite
```

Logs and stop:

```bash
docker logs -f fraud-simulator-run
docker stop fraud-simulator-run
```

Note: container name is always `fraud-simulator-run`.

## Fixed-Like Delay

Set min and max to same value:

```bash
python3 simulator/csv/realtime_csv_simulator.py \
	--input data/transaction_log.csv \
	--output data/realtime_transactions.csv \
	--interval-min 1.0 \
	--interval-max 1.0 \
	--max-events 100 \
	--overwrite
```
