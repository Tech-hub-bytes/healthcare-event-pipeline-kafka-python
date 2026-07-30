from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def patient_key_from_name(name: str | None) -> str:
    normalized = "".join(ch for ch in (name or "unknown").lower() if ch.isalnum())
    digest = hashlib.sha256((normalized or "unknown").encode("utf-8")).hexdigest()
    return digest[:16]


def build_envelope(
    *,
    event_type: str,
    source_system: str = "local-upload",
    patient_name: str | None = None,
    content_type: str = "application/xml",
    payload: str | None = None,
    payload_ref: str | None = None,
    phi_level: str = "restricted",
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    extras = extras or {}
    patient_key = patient_key_from_name(patient_name or extras.get("patient_hint") or "unknown")
    envelope: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": _utc_now(),
        "source_system": source_system,
        "patient_key": patient_key,
        "patient_display": patient_name,
        "content_type": content_type,
        "payload": payload,
        "payload_ref": payload_ref,
        "phi_level": phi_level,
        "trace_id": extras.get("trace_id") or str(uuid.uuid4()),
    }
    # Do not let extras overwrite core identity fields accidentally
    for key, value in extras.items():
        if key not in envelope:
            envelope[key] = value
    return envelope


def build_audit_event(
    *,
    action: str,
    event_id: str | None,
    trace_id: str | None,
    topic: str,
    detail: str,
) -> dict[str, Any]:
    return {
        "audit_id": str(uuid.uuid4()),
        "action": action,
        "event_id": event_id,
        "trace_id": trace_id,
        "topic": topic,
        "detail": detail,
        "at": _utc_now(),
    }
