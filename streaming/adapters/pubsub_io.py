from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event_envelope(
    *,
    payload: dict[str, Any],
    event_type: str,
    source: str,
    event_id: str | None = None,
    emitted_at_utc: str | None = None,
    schema_version: str = "1.0",
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "source": source,
        "emitted_at_utc": emitted_at_utc or utc_now_iso(),
        "payload": payload,
    }


def serialize_envelope(envelope: dict[str, Any]) -> bytes:
    return json.dumps(envelope, separators=(",", ":")).encode("utf-8")


def deserialize_envelope(raw: bytes) -> dict[str, Any]:
    decoded = json.loads(raw.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("Envelope must be a JSON object")

    if "payload" not in decoded or not isinstance(decoded["payload"], dict):
        raise ValueError("Envelope must include object payload")

    return decoded
