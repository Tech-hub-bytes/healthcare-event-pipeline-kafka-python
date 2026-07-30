from __future__ import annotations

import re
from typing import Any


def validate_ccda_envelope(envelope: dict[str, Any] | None) -> dict[str, Any]:
    """Return {ok, errors, warnings} for a C-CDA Kafka envelope."""
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(envelope, dict):
        return {"ok": False, "errors": ["Envelope is missing or not an object"], "warnings": warnings}

    if not envelope.get("event_id"):
        errors.append("Missing event_id")
    if not envelope.get("event_type"):
        errors.append("Missing event_type")
    if not envelope.get("patient_key"):
        errors.append("Missing patient_key")

    phi_level = envelope.get("phi_level")
    if phi_level not in ("restricted", "limited"):
        warnings.append(f"Unexpected phi_level: {phi_level}")

    xml = envelope.get("payload")
    if not isinstance(xml, str) or not xml.strip():
        errors.append("Missing payload XML string")
        return {"ok": False, "errors": errors, "warnings": warnings}

    if len(xml) > 5_000_000:
        errors.append("Payload exceeds 5MB limit")

    if not re.search(r"<([a-z0-9._-]+:)?ClinicalDocument\b", xml, re.I):
        errors.append("Not a ClinicalDocument (HL7 C-CDA) root")

    if not re.search(r"<([a-z0-9._-]+:)?recordTarget\b", xml, re.I):
        errors.append("Missing recordTarget / patient demographics section")

    if not re.search(r"<([a-z0-9._-]+:)?component\b", xml, re.I):
        warnings.append("No component sections found — document may be empty")

    if envelope.get("ssn") or envelope.get("social_security_number"):
        errors.append("SSN must not be placed on the envelope (PHI policy)")

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}
