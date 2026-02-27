#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="fraud-simulator"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONTAINER_NAME="fraud-simulator-run"

docker build -f "$PROJECT_ROOT/simulator/csv/Dockerfile" -t "$IMAGE_NAME" "$PROJECT_ROOT"

if [[ "$#" -eq 0 ]]; then
  set -- \
    --input data/transaction_log.csv \
    --output data/realtime_transactions.csv \
    --interval-min 0.3 \
    --interval-max 2.0 \
    --max-events 100 \
    --overwrite \
    --log-row-details
fi

docker run -d --rm \
  --name "$CONTAINER_NAME" \
  -v "$PROJECT_ROOT:/app" \
  "$IMAGE_NAME" \
  "$@"

echo "Container started in detached mode: $CONTAINER_NAME"
echo "View logs: docker logs -f $CONTAINER_NAME"
echo "Stop container: docker stop $CONTAINER_NAME"
