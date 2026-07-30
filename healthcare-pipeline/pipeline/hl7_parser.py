from __future__ import annotations

"""HL7 v2 ADT parser (minimal, pipe-delimited)."""

from datetime import datetime, timezone
from typing import Any


ADT_EVENT_NAMES = {
    "A01": "Admit",
    "A02": "Transfer",
    "A03": "Discharge",
    "A04": "Register",
    "A08": "Update patient info",
    "A11": "Cancel admit",
    "A13": "Cancel discharge",
}


def _seg_map(message: str) -> dict[str, list[str]]:
    segments: dict[str, list[str]] = {}
    # HL7 may use \r, \n, or \r\n as segment separators
    normalized = message.replace("\r\n", "\n").replace("\r", "\n")
    for line in normalized.split("\n"):
        line = line.strip()
        if not line or "|" not in line:
            continue
        name = line.split("|", 1)[0]
        segments.setdefault(name, []).append(line)
    return segments


def _fields(segment: str) -> list[str]:
    # Keep empty trailing fields
    return segment.split("|")


def _component(field: str, index: int = 0) -> str:
    if not field:
        return ""
    parts = field.split("^")
    return parts[index] if index < len(parts) else ""


def parse_hl7_adt(message: str, source_path: str = "inline.hl7") -> dict[str, Any]:
    segs = _seg_map(message)
    if "MSH" not in segs:
        raise ValueError("Missing MSH segment")
    if "PID" not in segs:
        raise ValueError("Missing PID segment")

    msh = _fields(segs["MSH"][0])
    # MSH field numbering: after split, [0]=MSH, [1]=|, [2]=encoding... actually
    # "MSH|^~\\&|..." -> ["MSH", "^~\\&", sending_app, ...] because first | after MSH
    # Standard: MSH-1 is field separator, MSH-2 is encoding chars = fields[1]
    message_type = msh[8] if len(msh) > 8 else ""
    trigger = _component(message_type, 1) or _component(message_type, 0)
    if trigger.startswith("ADT"):
        # e.g. ADT^A01 -> component0=ADT component1=A01
        trigger = _component(message_type, 1) or trigger

    pid = _fields(segs["PID"][0])
    # PID-3 patient ID, PID-5 name, PID-7 DOB, PID-8 sex
    patient_id = _component(pid[3], 0) if len(pid) > 3 else ""
    name_field = pid[5] if len(pid) > 5 else ""
    family = _component(name_field, 0)
    given = _component(name_field, 1)
    patient = f"{given} {family}".strip() or patient_id or "Unknown patient"
    dob = pid[7] if len(pid) > 7 else ""
    sex = pid[8] if len(pid) > 8 else ""

    pv1 = _fields(segs["PV1"][0]) if "PV1" in segs else []
    patient_class = pv1[2] if len(pv1) > 2 else ""
    location = _component(pv1[3], 0) if len(pv1) > 3 else ""
    attending = ""
    if len(pv1) > 7:
        attending = f"{_component(pv1[7], 2)} {_component(pv1[7], 1)}".strip()

    event_name = ADT_EVENT_NAMES.get(trigger, trigger or "ADT")
    silver = {
        "feed": "hl7_adt",
        "patient": patient,
        "patient_id": patient_id,
        "gender": sex or None,
        "birth_time": dob or None,
        "document_title": f"HL7 ADT^{trigger} — {event_name}",
        "source_path": source_path,
        "hl7_trigger": trigger,
        "hl7_event_name": event_name,
        "message_type": message_type,
        "patient_class": patient_class or None,
        "location": location or None,
        "attending": attending or None,
        "section_titles": [event_name, "Demographics", "Visit"],
        "section_count": 3,
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }

    chunks = [
        {
            "kind": "xml-overview",
            "path": f"{source_path}#overview",
            "patient": patient,
            "content": "\n".join(
                [
                    f"# {silver['document_title']}",
                    "",
                    "## Patient demographics",
                    f"- Name: {patient}",
                    f"- Patient ID: {patient_id}" if patient_id else "",
                    f"- Gender: {sex}" if sex else "",
                    f"- Date of birth: {dob}" if dob else "",
                    "",
                    "## Event",
                    f"- Trigger: ADT^{trigger} ({event_name})",
                    f"- Class: {patient_class}" if patient_class else "",
                    f"- Location: {location}" if location else "",
                    f"- Attending: {attending}" if attending else "",
                ]
            ).replace("\n\n\n", "\n\n"),
        },
        {
            "kind": "xml-section",
            "title": event_name,
            "path": f"{source_path}#event",
            "patient": patient,
            "content": (
                f"# {event_name}\n\nPatient: {patient}\n\n"
                f"HL7 trigger ADT^{trigger}. "
                f"Class={patient_class or 'n/a'}; Location={location or 'n/a'}."
            ),
        },
    ]

    return {"patient": patient, "chunks": chunks, "silver": silver, "format": "hl7"}


def validate_hl7_envelope(envelope: dict[str, Any] | None) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    if not isinstance(envelope, dict):
        return {"ok": False, "errors": ["Envelope is missing or not an object"], "warnings": warnings}
    if not envelope.get("event_id"):
        errors.append("Missing event_id")
    if not envelope.get("patient_key"):
        errors.append("Missing patient_key")
    payload = envelope.get("payload")
    if not isinstance(payload, str) or not payload.strip():
        errors.append("Missing HL7 payload string")
        return {"ok": False, "errors": errors, "warnings": warnings}
    if "MSH|" not in payload:
        errors.append("Not an HL7 v2 message (missing MSH|)")
    if "PID|" not in payload:
        errors.append("Missing PID segment")
    # Soft check for ADT
    if "ADT^" not in payload and "ADT|" not in payload:
        warnings.append("Message may not be an ADT trigger")
    if envelope.get("ssn") or envelope.get("social_security_number"):
        errors.append("SSN must not be placed on the envelope (PHI policy)")
    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}
