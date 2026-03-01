#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import smtplib
from email.message import EmailMessage

from kafka import KafkaConsumer

from consumers.env_loader import load_dotenv_if_exists


load_dotenv_if_exists()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Email notification consumer for fraud_alerts topic")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="fraud_alerts", help="Kafka topic name")
    parser.add_argument("--group-id", default="fraud-alerts-email-consumer", help="Consumer group id")
    parser.add_argument("--from-beginning", action="store_true", help="Read from beginning of topic")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages, 0 = no limit")

    parser.add_argument("--smtp-host", default=os.getenv("ALERT_SMTP_HOST", ""), help="SMTP host")
    parser.add_argument("--smtp-port", type=int, default=int(os.getenv("ALERT_SMTP_PORT", "587")), help="SMTP port")
    parser.add_argument("--smtp-user", default=os.getenv("ALERT_SMTP_USER", ""), help="SMTP username")
    parser.add_argument("--smtp-password", default=os.getenv("ALERT_SMTP_PASSWORD", ""), help="SMTP password")
    parser.add_argument("--email-from", default=os.getenv("ALERT_EMAIL_FROM", ""), help="Sender email")
    parser.add_argument("--email-to", default=os.getenv("ALERT_EMAIL_TO", ""), help="Receiver email")
    parser.add_argument(
        "--email-use-tls",
        dest="email_use_tls",
        action="store_true",
        default=env_bool("ALERT_EMAIL_USE_TLS", True),
        help="Use STARTTLS for SMTP",
    )
    parser.add_argument(
        "--no-email-use-tls",
        dest="email_use_tls",
        action="store_false",
        help="Disable STARTTLS",
    )
    return parser.parse_args()


def validate_required_config(args: argparse.Namespace) -> None:
    required = {
        "smtp_host": "ALERT_SMTP_HOST",
        "smtp_user": "ALERT_SMTP_USER",
        "smtp_password": "ALERT_SMTP_PASSWORD",
        "email_from": "ALERT_EMAIL_FROM",
        "email_to": "ALERT_EMAIL_TO",
    }
    missing = [f"--{field.replace('_', '-')} or {env_name}" for field, env_name in required.items() if not getattr(args, field)]
    if missing:
        joined = "\n- ".join(missing)
        raise SystemExit(f"Missing required email config:\n- {joined}")


def build_email_payload(alert: dict) -> tuple[str, str]:
    name_orig = alert.get("nameOrig", "UNKNOWN")
    amount = alert.get("amount", 0)
    score = alert.get("fraud_score", 0)
    tx_type = alert.get("type", "UNKNOWN")

    subject = f"[Fraud Alert] {tx_type} from {name_orig} score={float(score):.4f}"
    body = (
        "High-risk transaction detected.\n\n"
        f"nameOrig: {name_orig}\n"
        f"nameDest: {alert.get('nameDest', 'UNKNOWN')}\n"
        f"type: {tx_type}\n"
        f"amount: {amount}\n"
        f"fraud_score: {score}\n"
        f"predicted_is_fraud: {alert.get('predicted_is_fraud')}\n"
        f"is_alert: {alert.get('is_alert')}\n"
        f"event_ts: {alert.get('event_ts')}\n"
    )
    return subject, body


def send_email(args: argparse.Namespace, subject: str, body: str) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = args.email_from
    message["To"] = args.email_to
    message.set_content(body)

    with smtplib.SMTP(args.smtp_host, args.smtp_port, timeout=20) as server:
        if args.email_use_tls:
            server.starttls()
        server.login(args.smtp_user, args.smtp_password)
        server.send_message(message)


def main() -> None:
    args = parse_args()
    validate_required_config(args)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    )

    print(
        f"Email consumer listening topic='{args.topic}' bootstrap='{args.bootstrap_servers}' group='{args.group_id}'",
        flush=True,
    )

    consumed = 0
    sent = 0
    try:
        for message in consumer:
            consumed += 1
            alert = message.value

            subject, body = build_email_payload(alert)
            send_email(args, subject, body)
            sent += 1
            print(f"Email sent for message {consumed}", flush=True)

            if args.max_messages and consumed >= args.max_messages:
                break
    finally:
        consumer.close()
        print(f"Done. Consumed={consumed}, emailed={sent}", flush=True)


if __name__ == "__main__":
    main()
