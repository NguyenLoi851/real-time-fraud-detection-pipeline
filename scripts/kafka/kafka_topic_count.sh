#!/usr/bin/env bash
set -euo pipefail

TOPIC_NAME="${1:-transactions_raw}"
BOOTSTRAP_SERVER="${2:-localhost:9092}"

LATEST=$(docker exec fraud-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC_NAME" \
  --time -1 | awk -F: '{sum += $3} END {print sum + 0}')

EARLIEST=$(docker exec fraud-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC_NAME" \
  --time -2 | awk -F: '{sum += $3} END {print sum + 0}')

COUNT=$((LATEST - EARLIEST))

echo "Topic: $TOPIC_NAME"
echo "Messages: $COUNT"
