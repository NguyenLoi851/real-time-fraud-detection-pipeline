#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1

from streaming.adapters.pubsub_io import build_event_envelope, serialize_envelope

LABEL_FIELDS = {"isFraud", "isFlaggedFraud"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish CSV transactions to a Pub/Sub topic")
    parser.add_argument("--input", default="data/transaction_log.csv", help="Source CSV path")
    parser.add_argument("--project-id", required=True, help="GCP project id")
    parser.add_argument("--topic", default="transactions_raw", help="Pub/Sub topic id")
    parser.add_argument("--interval-min", type=float, default=0.3, help="Minimum delay between events")
    parser.add_argument("--interval-max", type=float, default=2.0, help="Maximum delay between events")
    parser.add_argument("--max-events", type=int, default=0, help="Maximum events to publish, 0 = no limit")
    parser.add_argument("--include-labels", action="store_true", help="Include isFraud/isFlaggedFraud in payload")
    parser.add_argument(
        "--event-start-utc",
        default="",
        help="Synthetic event start timestamp in ISO 8601 UTC (default: now)",
    )
    parser.add_argument(
        "--event-gap-seconds",
        type=float,
        default=3.0,
        help="Synthetic gap between consecutive event timestamps in seconds",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.interval_min < 0 or args.interval_max < 0:
        raise ValueError("--interval-min and --interval-max must be >= 0")
    if args.interval_min > args.interval_max:
        raise ValueError("--interval-min must be <= --interval-max")
    if args.max_events < 0:
        raise ValueError("--max-events must be >= 0")
    if args.event_gap_seconds < 0:
        raise ValueError("--event-gap-seconds must be >= 0")


def parse_event_start_utc(raw_value: str) -> datetime:
    if not raw_value:
        return datetime.now(timezone.utc)

    normalized = raw_value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def main() -> None:
    args = parse_args()
    validate_args(args)

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project_id, args.topic)

    synthetic_event_time = parse_event_start_utc(args.event_start_utc)
    published = 0

    with input_path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError(f"Input CSV has no header: {input_path}")

        for row in reader:
            if args.max_events and published >= args.max_events:
                break

            payload = {k: v for k, v in row.items() if args.include_labels or k not in LABEL_FIELDS}
            payload["event_emitted_at_utc"] = synthetic_event_time.isoformat()
            synthetic_event_time = synthetic_event_time + timedelta(seconds=args.event_gap_seconds)

            envelope = build_event_envelope(
                payload=payload,
                event_type="transaction.created",
                source="simulator.pubsub_csv_publisher",
            )

            data = serialize_envelope(envelope)
            try:
                future = publisher.publish(
                    topic_path,
                    data,
                    event_type=envelope["event_type"],
                    source=envelope["source"],
                    event_id=envelope["event_id"],
                )
                message_id = future.result(timeout=15)
            except GoogleAPICallError as exc:
                raise RuntimeError(f"Failed to publish to {topic_path}") from exc

            published += 1
            print(f"Published event {published} -> pubsub_message_id={message_id}", flush=True)
            time.sleep(random.uniform(args.interval_min, args.interval_max))

    print(f"Done. Published {published} events to topic '{args.topic}'", flush=True)


if __name__ == "__main__":
    main()
