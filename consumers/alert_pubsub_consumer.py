#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import smtplib
import time
from email.message import EmailMessage
from typing import Any
from urllib import request

from google.cloud import pubsub_v1

from consumers.env_loader import load_dotenv_if_exists
from streaming.adapters.pubsub_io import deserialize_envelope

load_dotenv_if_exists()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local Pub/Sub pull alert consumer")
    parser.add_argument(
        "--pubsub-project-id",
        default=os.getenv("GCP_PROJECT_ID", os.getenv("PUBSUB_PROJECT_ID", "")),
        help="GCP project id used when subscription name is not fully qualified",
    )
    parser.add_argument(
        "--pubsub-subscription",
        default=os.getenv("ALERT_PULL_SUBSCRIPTION", ""),
        help="Pub/Sub subscription name or full path (projects/<id>/subscriptions/<name>)",
    )
    parser.add_argument(
        "--pull-max-messages",
        type=int,
        default=int(os.getenv("ALERT_PULL_MAX_MESSAGES", "10")),
        help="Max messages to pull per request",
    )
    parser.add_argument(
        "--pull-timeout-seconds",
        type=float,
        default=float(os.getenv("ALERT_PULL_TIMEOUT_SECONDS", "15")),
        help="Pull request timeout in seconds",
    )
    parser.add_argument(
        "--idle-sleep-seconds",
        type=float,
        default=float(os.getenv("ALERT_PULL_IDLE_SLEEP_SECONDS", "2")),
        help="Sleep interval when no messages are returned",
    )
    parser.add_argument(
        "--ack-on-error",
        dest="ack_on_error",
        action="store_true",
        default=env_bool("ALERT_PULL_ACK_ON_ERROR", True),
        help="Acknowledge message even if processing fails",
    )
    parser.add_argument(
        "--no-ack-on-error",
        dest="ack_on_error",
        action="store_false",
        help="Do not acknowledge failed messages",
    )

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
    parser.add_argument("--no-email-use-tls", dest="email_use_tls", action="store_false", help="Disable STARTTLS")

    parser.add_argument(
        "--delivery-mode",
        choices=["email", "webhook", "both"],
        default=os.getenv("ALERT_DELIVERY_MODE", "email"),
        help="Alert delivery mode",
    )
    parser.add_argument("--webhook-url", default=os.getenv("ALERT_WEBHOOK_URL", ""), help="Webhook endpoint")
    return parser.parse_args()


def validate_required_config(args: argparse.Namespace) -> None:
    if args.delivery_mode in {"email", "both"}:
        required = {
            "smtp_host": "ALERT_SMTP_HOST",
            "smtp_user": "ALERT_SMTP_USER",
            "smtp_password": "ALERT_SMTP_PASSWORD",
            "email_from": "ALERT_EMAIL_FROM",
            "email_to": "ALERT_EMAIL_TO",
        }
        missing = [
            f"--{field.replace('_', '-')} or {env_name}"
            for field, env_name in required.items()
            if not getattr(args, field)
        ]
        if missing:
            raise SystemExit("Missing required email config:\n- " + "\n- ".join(missing))

    if args.delivery_mode in {"webhook", "both"} and not args.webhook_url:
        raise SystemExit("Missing webhook config: --webhook-url or ALERT_WEBHOOK_URL")

    if not args.pubsub_subscription:
        raise SystemExit("Missing pull subscription config: --pubsub-subscription or ALERT_PULL_SUBSCRIPTION")


def build_email_payload(alert: dict[str, Any]) -> tuple[str, str]:
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


def post_webhook(args: argparse.Namespace, payload: dict[str, Any]) -> None:
    req = request.Request(
        args.webhook_url,
        method="POST",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with request.urlopen(req, timeout=20) as response:
        if response.status >= 400:
            raise RuntimeError(f"Webhook returned status={response.status}")


def decode_pubsub_envelope_data(encoded_data: bytes) -> dict[str, Any]:
    envelope = deserialize_envelope(encoded_data)
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("Envelope payload must be an object")
    return payload


def process_alert_payload(args: argparse.Namespace, alert_payload: dict[str, Any]) -> None:
    subject, body = build_email_payload(alert_payload)
    if args.delivery_mode in {"email", "both"}:
        send_email(args, subject, body)
    if args.delivery_mode in {"webhook", "both"}:
        post_webhook(args, {"subject": subject, "body": body, "alert": alert_payload})


def build_subscription_path(args: argparse.Namespace, subscriber: pubsub_v1.SubscriberClient) -> str:
    subscription = args.pubsub_subscription.strip()
    if subscription.startswith("projects/"):
        return subscription
    if not args.pubsub_project_id:
        raise SystemExit(
            "When using short subscription name, set --pubsub-project-id or GCP_PROJECT_ID/PUBSUB_PROJECT_ID"
        )
    return subscriber.subscription_path(args.pubsub_project_id, subscription)


def run_pull_loop(args: argparse.Namespace) -> None:
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = build_subscription_path(args, subscriber)
    print(
        (
            "Starting local Pub/Sub pull alert consumer "
            f"subscription={subscription_path} delivery_mode={args.delivery_mode}"
        ),
        flush=True,
    )

    while True:
        try:
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": args.pull_max_messages},
                timeout=args.pull_timeout_seconds,
            )
        except KeyboardInterrupt:
            print("Stopping Pub/Sub pull consumer", flush=True)
            break
        except Exception as exc:
            print(f"Pull request failed: {exc}", flush=True)
            time.sleep(max(args.idle_sleep_seconds, 0.1))
            continue

        messages = list(response.received_messages)
        if not messages:
            time.sleep(max(args.idle_sleep_seconds, 0.1))
            continue

        ack_ids: list[str] = []
        for received in messages:
            ack_this = False
            try:
                alert_payload = decode_pubsub_envelope_data(received.message.data)
                process_alert_payload(args, alert_payload)
                ack_this = True
            except Exception as exc:
                print(f"Failed to process pulled alert message_id={received.message.message_id}: {exc}", flush=True)
                ack_this = args.ack_on_error

            if ack_this:
                ack_ids.append(received.ack_id)

        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})


def main() -> None:
    args = parse_args()
    validate_required_config(args)
    run_pull_loop(args)


if __name__ == "__main__":
    main()
