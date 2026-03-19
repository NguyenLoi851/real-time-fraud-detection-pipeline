#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import time
from datetime import datetime, timedelta, timezone
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
    parser.add_argument(
        "--event-time-mode",
        choices=["wall-clock", "synthetic"],
        default="synthetic",
        help="Use wall-clock time or synthetic event time progression",
    )
    parser.add_argument(
        "--event-start-utc",
        default="",
        help="Synthetic event start timestamp in ISO 8601 UTC (default: now)",
    )
    parser.add_argument(
        "--event-gap-min-seconds",
        type=float,
        default=1.0,
        help="Minimum synthetic gap between consecutive events in seconds",
    )
    parser.add_argument(
        "--event-gap-max-seconds",
        type=float,
        default=10.0,
        help="Maximum synthetic gap between consecutive events in seconds",
    )
    parser.add_argument(
        "--event-gap-distribution",
        choices=["uniform", "log-uniform", "triangular"],
        default="triangular",
        help="Distribution for synthetic gaps",
    )
    parser.add_argument(
        "--event-gap-mode-seconds",
        type=float,
        default=5.0,
        help="Mode used by triangular distribution (default: 2 hour)",
    )
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
    if args.event_gap_min_seconds < 0 or args.event_gap_max_seconds < 0:
        raise ValueError("--event-gap-min-seconds and --event-gap-max-seconds must be >= 0")
    if args.event_gap_min_seconds > args.event_gap_max_seconds:
        raise ValueError("--event-gap-min-seconds must be <= --event-gap-max-seconds")
    if args.event_gap_mode_seconds < 0:
        raise ValueError("--event-gap-mode-seconds must be >= 0")
    if args.event_gap_distribution == "triangular":
        if not (args.event_gap_min_seconds <= args.event_gap_mode_seconds <= args.event_gap_max_seconds):
            raise ValueError(
                "--event-gap-mode-seconds must be between --event-gap-min-seconds and --event-gap-max-seconds for triangular distribution"
            )
    if args.event_time_mode == "synthetic" and args.event_gap_distribution == "log-uniform" and args.event_gap_max_seconds == 0:
        raise ValueError("--event-gap-max-seconds must be > 0 for log-uniform distribution")
    if args.max_events < 0:
        raise ValueError("--max-events must be >= 0")


def parse_event_start_utc(raw_value: str) -> datetime:
    if not raw_value:
        return datetime.now(timezone.utc)

    normalized = raw_value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("--event-start-utc must be a valid ISO 8601 timestamp") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def sample_synthetic_gap_seconds(args: argparse.Namespace) -> float:
    minimum = args.event_gap_min_seconds
    maximum = args.event_gap_max_seconds
    if minimum == maximum:
        return minimum

    if args.event_gap_distribution == "uniform":
        return random.uniform(minimum, maximum)

    if args.event_gap_distribution == "triangular":
        return random.triangular(minimum, maximum, args.event_gap_mode_seconds)

    if maximum <= 0:
        return 0.0

    log_min = math.log(max(minimum, 1e-6))
    log_max = math.log(maximum)
    return math.exp(random.uniform(log_min, log_max))


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

    synthetic_event_time = parse_event_start_utc(args.event_start_utc)

    published = 0
    with input_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError(f"Input CSV has no header: {input_path}")

        for row in reader:
            if args.max_events and published >= args.max_events:
                break

            event = {k: v for k, v in row.items() if args.include_labels or k not in LABEL_FIELDS}
            if args.event_time_mode == "synthetic":
                event["event_emitted_at_utc"] = synthetic_event_time.isoformat()
                synthetic_event_time = synthetic_event_time + timedelta(seconds=sample_synthetic_gap_seconds(args))
            else:
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
