#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AIRFLOW_DIR="$ROOT_DIR/airflow"

cd "$AIRFLOW_DIR"
docker compose -f docker-compose.airflow.yml down -v --remove-orphans

if [[ -d "$AIRFLOW_DIR/logs" ]]; then
	find "$AIRFLOW_DIR/logs" -mindepth 1 -delete
fi

echo "[airflow] Removed containers, network, postgres volume, and old logs."
