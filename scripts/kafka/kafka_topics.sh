#!/usr/bin/env bash
set -euo pipefail

docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 "$@"
