#!/usr/bin/env bash
set -euo pipefail

TOPIC_NAME="${1:-transactions_raw}"
PARTITIONS="${2:-1}"
REPLICATION_FACTOR="${3:-1}"

docker exec fraud-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC_NAME" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

echo "Topic ready: $TOPIC_NAME"
