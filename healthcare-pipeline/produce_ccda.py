from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka import Producer

from pipeline.ccda_parser import parse_ccda_xml
from pipeline.config import KAFKA_BROKERS, PATHS, TOPICS
from pipeline.envelope import build_audit_event, build_envelope
from pipeline.schema_guard import validate_local


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")


def main() -> None:
    file_arg = sys.argv[1] if len(sys.argv) > 1 else None
    file_path = Path(file_arg).resolve() if file_arg else PATHS["samples"] / "emma-ccda.xml"
    xml = file_path.read_text(encoding="utf-8")
    file_name = file_path.name

    patient_hint = file_name.replace(".xml", "").replace("_", " ").replace("-", " ")
    try:
        preview = parse_ccda_xml(xml, file_name)
        patient_hint = preview["patient"] or patient_hint
    except Exception:  # noqa: BLE001
        pass

    envelope = build_envelope(
        event_type="ccda.document.received",
        source_system="local-file-producer",
        patient_name=patient_hint,
        content_type="application/xml",
        payload=xml,
        extras={"source_file": file_name, "source_path": str(file_path)},
    )
    schema_errors = validate_local(envelope, "envelope.schema.json")
    if schema_errors:
        raise SystemExit(f"Envelope schema validation failed: {'; '.join(schema_errors)}")

    producer = Producer({"bootstrap.servers": KAFKA_BROKERS, "client.id": "healthcare-producer-py"})
    producer.produce(
        TOPICS["ccda_raw"],
        key=envelope["patient_key"].encode("utf-8"),
        value=json.dumps(envelope).encode("utf-8"),
        callback=delivery_report,
    )
    audit = build_audit_event(
        action="produced",
        event_id=envelope["event_id"],
        trace_id=envelope["trace_id"],
        topic=TOPICS["ccda_raw"],
        detail=f"Produced {file_name} for patient_key={envelope['patient_key']}",
    )
    producer.produce(
        TOPICS["audit"],
        key=envelope["event_id"].encode("utf-8"),
        value=json.dumps(audit).encode("utf-8"),
        callback=delivery_report,
    )
    producer.flush(10)

    print("Produced C-CDA event")
    print(f"  event_id:    {envelope['event_id']}")
    print(f"  patient:     {envelope['patient_display']}")
    print(f"  patient_key: {envelope['patient_key']}")
    print(f"  topic:       {TOPICS['ccda_raw']}")
    print(f"  file:        {file_path}")


if __name__ == "__main__":
    main()
