#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

ASSUME_YES=0
DRY_RUN=0
ANY_SERVICE_SELECTED=0

CLEAN_KAFKA=0
CLEAN_SPARK=0
CLEAN_AIRFLOW=0
CLEAN_DBT=0
CLEAN_TERRAFORM=0
CLEAN_SIMULATOR=0

usage() {
  cat <<'EOF'
Usage:
  ./scripts/cleanup/reset_generated_data.sh [--yes] [--dry-run] [service options]

Options:
  --yes       Skip confirmation prompt.
  --dry-run   Show actions without deleting anything.
  --kafka     Cleanup Kafka-related runtime (stop Kafka stack).
  --spark     Cleanup Spark-generated checkpoints and lake outputs.
  --airflow   Cleanup Airflow runtime (stop Airflow, clear logs).
  --dbt       Cleanup dbt artifacts (`logs`, `target`).
  --terraform Cleanup Terraform local artifacts/state.
  --simulator Cleanup simulator runtime data/container.
  -h, --help  Show help.

Examples:
  ./scripts/cleanup/reset_generated_data.sh --yes
  ./scripts/cleanup/reset_generated_data.sh --yes --spark --terraform
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
    --kafka)
      ANY_SERVICE_SELECTED=1
      CLEAN_KAFKA=1
      ;;
    --spark)
      ANY_SERVICE_SELECTED=1
      CLEAN_SPARK=1
      ;;
    --airflow)
      ANY_SERVICE_SELECTED=1
      CLEAN_AIRFLOW=1
      ;;
    --dbt)
      ANY_SERVICE_SELECTED=1
      CLEAN_DBT=1
      ;;
    --terraform)
      ANY_SERVICE_SELECTED=1
      CLEAN_TERRAFORM=1
      ;;
    --simulator)
      ANY_SERVICE_SELECTED=1
      CLEAN_SIMULATOR=1
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

if [[ "$ANY_SERVICE_SELECTED" -eq 0 ]]; then
  CLEAN_KAFKA=1
  CLEAN_SPARK=1
  CLEAN_AIRFLOW=1
  CLEAN_DBT=1
  CLEAN_TERRAFORM=1
  CLEAN_SIMULATOR=1
fi

SELECTED_SERVICES=()
[[ "$CLEAN_KAFKA" -eq 1 ]] && SELECTED_SERVICES+=("kafka")
[[ "$CLEAN_SPARK" -eq 1 ]] && SELECTED_SERVICES+=("spark")
[[ "$CLEAN_AIRFLOW" -eq 1 ]] && SELECTED_SERVICES+=("airflow")
[[ "$CLEAN_DBT" -eq 1 ]] && SELECTED_SERVICES+=("dbt")
[[ "$CLEAN_TERRAFORM" -eq 1 ]] && SELECTED_SERVICES+=("terraform")
[[ "$CLEAN_SIMULATOR" -eq 1 ]] && SELECTED_SERVICES+=("simulator")

TARGET_LABEL="${SELECTED_SERVICES[*]}"

if [[ "$ASSUME_YES" -ne 1 ]]; then
  echo "This will remove generated local runtime data for: $TARGET_LABEL"
  read -r -p "Continue? [y/N] " reply
  if [[ ! "$reply" =~ ^[Yy]$ ]]; then
    log "Aborted."
    exit 0
  fi
fi

cd "$PROJECT_ROOT"

log "Selected cleanup targets: $TARGET_LABEL"

if [[ "$CLEAN_KAFKA" -eq 1 ]]; then
  log "Stopping Kafka runtime services."
  if [[ -x "$PROJECT_ROOT/scripts/kafka/kafka_down.sh" ]]; then
    run_cmd "\"$PROJECT_ROOT/scripts/kafka/kafka_down.sh\" || true"
  fi
fi

if [[ "$CLEAN_AIRFLOW" -eq 1 ]]; then
  log "Stopping Airflow runtime services and clearing logs."
  if [[ -x "$PROJECT_ROOT/scripts/airflow/airflow_down.sh" ]]; then
    run_cmd "\"$PROJECT_ROOT/scripts/airflow/airflow_down.sh\" || true"
  fi
  clear_dir_contents "$PROJECT_ROOT/airflow/logs"
fi

if [[ "$CLEAN_SIMULATOR" -eq 1 ]]; then
  log "Cleaning simulator runtime data."
  run_cmd "docker rm -f fraud-simulator-run >/dev/null 2>&1 || true"
  delete_file_if_exists "$PROJECT_ROOT/data/realtime_transactions.csv"
fi

if [[ "$CLEAN_SPARK" -eq 1 ]]; then
  log "Cleaning Spark generated checkpoints and lake outputs."
  clear_dir_contents "$PROJECT_ROOT/data/checkpoints"
  clear_dir_contents "$PROJECT_ROOT/data/lake/bronze/transactions_raw"
  clear_dir_contents "$PROJECT_ROOT/data/lake/silver/scored_transactions"
  clear_dir_contents "$PROJECT_ROOT/data/lake/gold/fraud_alerts"
  clear_dir_contents "$PROJECT_ROOT/data/lake/gold/hourly_batch"
  run_cmd "find \"$PROJECT_ROOT\" -type d -name __pycache__ -prune -exec rm -rf {} +"
fi

if [[ "$CLEAN_DBT" -eq 1 ]]; then
  log "Cleaning dbt build artifacts."
  clear_dir_contents "$PROJECT_ROOT/dbt/logs"
  clear_dir_contents "$PROJECT_ROOT/dbt/target"
fi

if [[ "$CLEAN_TERRAFORM" -eq 1 ]]; then
  log "Cleaning Terraform local artifacts/state."
  clear_dir_contents "$PROJECT_ROOT/infra/terraform/.terraform"

  delete_file_if_exists "$PROJECT_ROOT/infra/terraform/terraform.tfstate"
  delete_file_if_exists "$PROJECT_ROOT/infra/terraform/terraform.tfstate.backup"
  delete_file_if_exists "$PROJECT_ROOT/infra/terraform/.terraform.tfstate.lock.info"
fi

log "Cleanup complete."
