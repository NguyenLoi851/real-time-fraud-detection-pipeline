#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ASSUME_YES=0
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/cleanup/reset_generated_data.sh [--yes] [--dry-run]

Options:
  --yes       Skip confirmation prompt.
  --dry-run   Show actions without deleting anything.
  -h, --help  Show help.

Examples:
  ./scripts/cleanup/reset_generated_data.sh --yes
EOF
}

log() {
  echo "[cleanup] $*"
}

run_cmd() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] $*"
  else
    eval "$*"
  fi
}

clear_dir_contents() {
  local dir_path="$1"
  if [[ -d "$dir_path" ]]; then
    run_cmd "find \"$dir_path\" -mindepth 1 -delete"
  fi
}

delete_file_if_exists() {
  local file_path="$1"
  if [[ -f "$file_path" ]]; then
    run_cmd "rm -f \"$file_path\""
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes)
      ASSUME_YES=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ "$ASSUME_YES" -ne 1 ]]; then
  echo "This will remove generated local runtime data."
  read -r -p "Continue? [y/N] " reply
  if [[ ! "$reply" =~ ^[Yy]$ ]]; then
    log "Aborted."
    exit 0
  fi
fi

cd "$PROJECT_ROOT"

log "Stopping local runtime services (Kafka, Airflow, simulator container)."
if [[ -x "$PROJECT_ROOT/scripts/kafka/kafka_down.sh" ]]; then
  run_cmd "\"$PROJECT_ROOT/scripts/kafka/kafka_down.sh\" || true"
fi
if [[ -x "$PROJECT_ROOT/scripts/airflow/airflow_down.sh" ]]; then
  run_cmd "\"$PROJECT_ROOT/scripts/airflow/airflow_down.sh\" || true"
fi
run_cmd "docker rm -f fraud-simulator-run >/dev/null 2>&1 || true"

log "Clearing local generated data."
clear_dir_contents "$PROJECT_ROOT/data/checkpoints"
clear_dir_contents "$PROJECT_ROOT/data/lake/bronze/transactions_raw"
clear_dir_contents "$PROJECT_ROOT/data/lake/silver/scored_transactions"
clear_dir_contents "$PROJECT_ROOT/data/lake/gold/fraud_alerts"
clear_dir_contents "$PROJECT_ROOT/data/lake/gold/hourly_batch"
clear_dir_contents "$PROJECT_ROOT/airflow/logs"
clear_dir_contents "$PROJECT_ROOT/dbt/logs"
clear_dir_contents "$PROJECT_ROOT/dbt/target"

delete_file_if_exists "$PROJECT_ROOT/data/realtime_transactions.csv"

run_cmd "find \"$PROJECT_ROOT\" -type d -name __pycache__ -prune -exec rm -rf {} +"

log "Cleanup complete."
