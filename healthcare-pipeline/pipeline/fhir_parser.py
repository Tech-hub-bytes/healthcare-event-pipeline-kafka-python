from __future__ import annotations

"""FHIR R4 resource parser/normalizer (Patient, Encounter, Observation, Bundle)."""

import json
from datetime import datetime, timezone
from typing import Any


def _human_name(name_list: list[dict[str, Any]] | None) -> str:
    if not name_list:
        return ""
    n = name_list[0]
    family = n.get("family") or ""
    given = " ".join(n.get("given") or [])
    return f"{given} {family}".strip()


def _patient_from_resource(res: dict[str, Any]) -> tuple[str, str | None, str | None, str | None]:
    patient = _human_name(res.get("name")) or res.get("id") or "Unknown patient"
    gender = res.get("gender")
    birth = res.get("birthDate")
    pid = None
    for ident in res.get("identifier") or []:
        if ident.get("value"):
            pid = str(ident["value"])
            break
    if not pid:
        pid = res.get("id")
    return patient, gender, birth, pid


def _extract_resources(payload_obj: dict[str, Any]) -> list[dict[str, Any]]:
    rtype = payload_obj.get("resourceType")
    if rtype == "Bundle":
        out = []
        for entry in payload_obj.get("entry") or []:
            res = entry.get("resource")
            if isinstance(res, dict) and res.get("resourceType"):
                out.append(res)
        return out
    if rtype:
        return [payload_obj]
    return []


def parse_fhir_resource(payload: str | dict[str, Any], source_path: str = "inline.json") -> dict[str, Any]:
    if isinstance(payload, str):
        obj = json.loads(payload)
    else:
        obj = payload

    resources = _extract_resources(obj)
    if not resources:
        raise ValueError("No FHIR resources found")

    patient = "Unknown patient"
    gender = None
    birth = None
    patient_id = None
    encounters: list[str] = []
    observations: list[str] = []
    resource_types: list[str] = []

    for res in resources:
        rtype = res.get("resourceType") or "Resource"
        resource_types.append(rtype)
        if rtype == "Patient":
            patient, gender, birth, patient_id = _patient_from_resource(res)
        elif rtype == "Encounter":
            status = res.get("status") or ""
            klass = (res.get("class") or {}).get("code") or ""
            encounters.append(f"Encounter {res.get('id', '')} status={status} class={klass}".strip())
        elif rtype == "Observation":
            code = res.get("code") or {}
            coding = (code.get("coding") or [{}])[0]
            display = code.get("text") or coding.get("display") or coding.get("code") or "Observation"
            value = None
            if "valueQuantity" in res:
                vq = res["valueQuantity"]
                value = f"{vq.get('value')} {vq.get('unit', '')}".strip()
            elif "valueString" in res:
                value = res["valueString"]
            observations.append(f"{display}: {value}" if value else display)

    # If only Encounter/Observation without Patient, try subject display
    if patient == "Unknown patient":
        for res in resources:
            subj = res.get("subject") or {}
            if subj.get("display"):
                patient = subj["display"]
                break

    section_titles = list(dict.fromkeys(resource_types))
    silver = {
        "feed": "fhir",
        "patient": patient,
        "patient_id": patient_id,
        "gender": gender,
        "birth_time": birth,
        "document_title": f"FHIR {', '.join(section_titles)}",
        "source_path": source_path,
        "resource_types": section_titles,
        "encounter_summaries": encounters,
        "observation_summaries": observations,
        "section_titles": section_titles,
        "section_count": len(section_titles),
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }

    lines = [
        f"# {silver['document_title']}",
        "",
        "## Patient demographics",
        f"- Name: {patient}",
        f"- Patient ID: {patient_id}" if patient_id else "",
        f"- Gender: {gender}" if gender else "",
        f"- Date of birth: {birth}" if birth else "",
        "",
        "## Resources",
        *[f"- {t}" for t in section_titles],
    ]
    if encounters:
        lines += ["", "## Encounters", *[f"- {e}" for e in encounters]]
    if observations:
        lines += ["", "## Observations", *[f"- {o}" for o in observations]]

    chunks = [
        {
            "kind": "xml-overview",
            "path": f"{source_path}#overview",
            "patient": patient,
            "content": "\n".join([ln for ln in lines if ln is not None]).replace("\n\n\n", "\n\n"),
        }
    ]
    for title, body_lines in (
        ("Encounters", encounters),
        ("Observations", observations),
    ):
        if not body_lines:
            continue
        chunks.append(
            {
                "kind": "xml-section",
                "title": title,
                "path": f"{source_path}#{title.lower()}",
                "patient": patient,
                "content": f"# {title}\n\nPatient: {patient}\n\n" + "\n".join(f"- {b}" for b in body_lines),
            }
        )

    return {"patient": patient, "chunks": chunks, "silver": silver, "format": "fhir"}


def validate_fhir_envelope(envelope: dict[str, Any] | None) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    if not isinstance(envelope, dict):
        return {"ok": False, "errors": ["Envelope is missing or not an object"], "warnings": warnings}
    if not envelope.get("event_id"):
        errors.append("Missing event_id")
    if not envelope.get("patient_key"):
        errors.append("Missing patient_key")

    payload = envelope.get("payload")
    obj: dict[str, Any] | None = None
    if isinstance(payload, dict):
        obj = payload
    elif isinstance(payload, str) and payload.strip():
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                obj = parsed
            else:
                errors.append("FHIR payload must be a JSON object")
        except json.JSONDecodeError:
            errors.append("FHIR payload is not valid JSON")
    else:
        errors.append("Missing FHIR payload")

    if obj is not None:
        rtype = obj.get("resourceType")
        if not rtype:
            errors.append("Missing resourceType")
        elif rtype not in ("Patient", "Encounter", "Observation", "Bundle", "MedicationRequest"):
            warnings.append(f"Unusual resourceType for this pipeline: {rtype}")
        if rtype == "Bundle" and not (obj.get("entry") or []):
            errors.append("Bundle has no entries")

    if envelope.get("ssn") or envelope.get("social_security_number"):
        errors.append("SSN must not be placed on the envelope (PHI policy)")

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}
