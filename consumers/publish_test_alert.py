#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from consumers.env_loader import load_dotenv_if_exists


load_dotenv_if_exists()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish dummy fraud alert messages for consumer testing")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="fraud_alerts", help="Kafka topic name")
    parser.add_argument("--count", type=int, default=1, help="Number of test messages to publish")
    parser.add_argument("--base-score", type=float, default=0.91, help="Base fraud score for generated messages")
    parser.add_argument("--amount", type=float, default=250000.0, help="Transaction amount for generated messages")
    parser.add_argument("--name-orig", default="C_TEST_ORIG", help="Origin account/customer")
    parser.add_argument("--name-dest", default="C_TEST_DEST", help="Destination account/customer")
    return parser.parse_args()


def build_message(index: int, args: argparse.Namespace) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    score = min(max(args.base_score + (index * 0.001), 0.0), 1.0)
    random_suffix_orig = uuid.uuid4().hex[:6].upper()
    random_suffix_dest = uuid.uuid4().hex[:6].upper()

    amount = round(random.uniform(max(args.amount * 0.4, 1.0), args.amount * 1.8), 2)
    old_balance_org = round(random.uniform(amount + 1000.0, max(amount * 4.0, 10000.0)), 2)
    origin_balance_delta = round(amount * random.uniform(0.85, 1.05), 2)
    new_balance_orig = round(max(old_balance_org - origin_balance_delta, 0.0), 2)

    old_balance_dest = round(random.uniform(0.0, 50000.0), 2)
    dest_balance_delta = round(amount * random.uniform(0.9, 1.1), 2)
    new_balance_dest = round(old_balance_dest + dest_balance_delta, 2)

    velocity_5min = round(random.uniform(1.0, 20.0), 2)
    is_new_merchant = 1.0 if random.random() < 0.3 else 0.0
    balance_change_ratio = round(amount / old_balance_org, 4) if old_balance_org > 0 else 0.0

    return {
        "source_topic": "transactions_raw",
        "source_partition": 0,
        "source_offset": index,
        "kafka_ingest_ts": now_iso,
        "event_emitted_at_utc": now_iso,
        "event_ts": now_iso,
        "step": "999",
        "type": "TRANSFER",
        "amount": amount,
        "nameOrig": f"{args.name_orig}_{index}_{random_suffix_orig}",
        "nameDest": f"{args.name_dest}_{index}_{random_suffix_dest}",
        "oldbalanceOrg": old_balance_org,
        "newbalanceOrig": new_balance_orig,
        "oldbalanceDest": old_balance_dest,
        "newbalanceDest": new_balance_dest,
        "velocity_5min": velocity_5min,
        "balance_change_ratio": balance_change_ratio,
        "is_new_merchant": is_new_merchant,
        "origin_balance_delta": origin_balance_delta,
        "dest_balance_delta": dest_balance_delta,
        "fraud_score": round(score, 4),
        "predicted_is_fraud": True,
        "is_alert": True,
        "rule_high_score": True,
        "rule_high_amount_transfer": True,
        "rule_high_velocity": True,
    }


def main() -> None:
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    for index in range(1, args.count + 1):
        payload = build_message(index, args)
        producer.send(args.topic, key=payload["nameOrig"].encode("utf-8"), value=payload)

    producer.flush()
    producer.close()
    print(
        f"Published {args.count} test alert message(s) to topic='{args.topic}' on '{args.bootstrap_servers}'",
        flush=True,
    )


if __name__ == "__main__":
    main()
