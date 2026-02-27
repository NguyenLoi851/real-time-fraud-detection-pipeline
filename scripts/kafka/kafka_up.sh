#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"
docker compose -f simulator/kafka/docker-compose.kafka.yml up -d

echo "Kafka is starting in detached mode."
echo "Check container: docker ps --filter name=fraud-kafka"
echo "Follow logs: docker logs -f fraud-kafka"
