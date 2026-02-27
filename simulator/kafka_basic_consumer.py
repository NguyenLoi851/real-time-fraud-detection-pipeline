#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from kafka import KafkaConsumer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Basic Kafka consumer for topic verification")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="transactions_raw", help="Kafka topic name")
    parser.add_argument("--group-id", default="fraud-debug-consumer", help="Consumer group id")
    parser.add_argument("--from-beginning", action="store_true", help="Read from beginning of topic")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages, 0 = no limit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    )

    print(
        f"Listening topic='{args.topic}' bootstrap='{args.bootstrap_servers}' group='{args.group_id}'",
        flush=True,
    )

    consumed = 0
    try:
        for message in consumer:
            consumed += 1
            print(
                f"Message {consumed}: partition={message.partition} offset={message.offset} value={message.value}",
                flush=True,
            )
            if args.max_messages and consumed >= args.max_messages:
                break
    finally:
        consumer.close()
        print(f"Done. Consumed {consumed} messages", flush=True)


if __name__ == "__main__":
    main()
