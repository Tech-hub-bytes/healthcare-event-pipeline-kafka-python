from __future__ import annotations

import re
from typing import Any

from pipeline.hardening import ALLOW_PATIENT_DISPLAY_IN_LOGS, REDACT_PHI_IN_LOGS

_MRN_RE = re.compile(r"\bMRN[-_]?[A-Z0-9]+\b", re.I)
_DOB_RE = re.compile(r"\b(?:19|20)\d{2}[-/]?(?:0[1-9]|1[0-2])[-/]?(?:0[1-9]|[12]\d|3[01])\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_PHONE_RE = re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


def redact_text(text: str) -> str:
    if not REDACT_PHI_IN_LOGS or not text:
        return text
    out = _SSN_RE.sub("[REDACTED_SSN]", text)
    out = _MRN_RE.sub("[REDACTED_MRN]", out)
    out = _DOB_RE.sub("[REDACTED_DOB]", out)
    out = _PHONE_RE.sub("[REDACTED_PHONE]", out)
    out = _EMAIL_RE.sub("[REDACTED_EMAIL]", out)
    return out


def redact_envelope_for_log(envelope: dict[str, Any] | None) -> dict[str, Any]:
    if not envelope:
        return {}
    safe = {
        "event_id": envelope.get("event_id"),
        "event_type": envelope.get("event_type"),
        "patient_key": envelope.get("patient_key"),
        "trace_id": envelope.get("trace_id"),
        "feed": envelope.get("feed"),
        "content_type": envelope.get("content_type"),
        "phi_level": envelope.get("phi_level"),
        "source_system": envelope.get("source_system"),
        "payload_bytes": len(str(envelope.get("payload") or "")),
    }
    if ALLOW_PATIENT_DISPLAY_IN_LOGS:
        safe["patient_display"] = envelope.get("patient_display")
    else:
        safe["patient_display"] = "[REDACTED]"
    return safe


def safe_log(msg: str) -> str:
    return redact_text(msg)
