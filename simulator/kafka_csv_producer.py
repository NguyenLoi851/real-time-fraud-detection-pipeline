#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


LABEL_FIELDS = {"isFraud", "isFlaggedFraud"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish CSV transactions to Kafka topic in real-time style."
    )
    parser.add_argument("--input", default="data/transaction_log.csv", help="Source CSV path")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="transactions_raw", help="Kafka topic name")
    parser.add_argument("--interval-min", type=float, default=0.3, help="Minimum delay between events")
    parser.add_argument("--interval-max", type=float, default=2.0, help="Maximum delay between events")
    parser.add_argument("--max-events", type=int, default=0, help="Maximum events to publish, 0 = no limit")
    parser.add_argument(
        "--include-labels",
        action="store_true",
        help="Include isFraud/isFlaggedFraud in produced messages",
    )
    parser.add_argument("--log-row-details", action="store_true", help="Print row summary for each event")
    return parser.parse_args()


def row_log_summary(row: dict[str, str], include_labels: bool) -> str:
    parts = [
        f"step={row.get('step', '')}",
        f"type={row.get('type', '')}",
        f"amount={row.get('amount', '')}",
        f"nameOrig={row.get('nameOrig', '')}",
        f"nameDest={row.get('nameDest', '')}",
    ]
    if include_labels:
        parts.append(f"isFraud={row.get('isFraud', '')}")
        parts.append(f"isFlaggedFraud={row.get('isFlaggedFraud', '')}")
    return ", ".join(parts)


def validate_args(args: argparse.Namespace) -> None:
    if args.interval_min < 0 or args.interval_max < 0:
        raise ValueError("--interval-min and --interval-max must be >= 0")
    if args.interval_min > args.interval_max:
        raise ValueError("--interval-min must be <= --interval-max")
    if args.max_events < 0:
        raise ValueError("--max-events must be >= 0")


def main() -> None:
    args = parse_args()
    validate_args(args)

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        acks="all",
    )

    published = 0
    with input_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError(f"Input CSV has no header: {input_path}")

        for row in reader:
            if args.max_events and published >= args.max_events:
                break

            event = {k: v for k, v in row.items() if args.include_labels or k not in LABEL_FIELDS}
            event["event_emitted_at_utc"] = datetime.now(timezone.utc).isoformat()

            future = producer.send(args.topic, value=event)
            metadata = future.get(timeout=10)

            published += 1
            print(
                f"Published event {published} -> topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}",
                flush=True,
            )
            if args.log_row_details:
                print(f"Event details: {row_log_summary(row, args.include_labels)}", flush=True)

            time.sleep(random.uniform(args.interval_min, args.interval_max))

    producer.flush()
    producer.close()
    print(f"Done. Published {published} events to topic '{args.topic}'", flush=True)


if __name__ == "__main__":
    main()
