#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AIRFLOW_DIR="$ROOT_DIR/airflow"

if [[ ! -f "$AIRFLOW_DIR/.env" ]]; then
  echo "[airflow] Missing airflow/.env. Copy airflow/.env.example first." >&2
  exit 1
fi

cd "$AIRFLOW_DIR"
docker compose -f docker-compose.airflow.yml up -d --build

echo "[airflow] Web UI: http://localhost:8080"
echo "[airflow] Default login: admin / admin"
