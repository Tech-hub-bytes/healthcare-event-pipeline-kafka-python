from __future__ import annotations

import json
import re
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from pipeline.ccda_parser import parse_ccda_xml
from pipeline.config import KAFKA_BROKERS, PATHS, RAW_TOPICS, TOPICS
from pipeline.envelope import build_audit_event
from pipeline.fhir_parser import parse_fhir_resource, validate_fhir_envelope
from pipeline.hardening import STRIP_PAYLOAD_FROM_BRONZE
from pipeline.hl7_parser import parse_hl7_adt, validate_hl7_envelope
from pipeline.phi import redact_envelope_for_log, safe_log
from pipeline.schema_guard import validate_local
from pipeline.validator import validate_ccda_envelope

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        pass

RUNNING = True


def _stop(*_args: object) -> None:
    global RUNNING
    RUNNING = False


def ensure_dirs() -> None:
    for p in PATHS.values():
        Path(p).mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def land_success(envelope: dict[str, Any], parsed: dict[str, Any], ext: str) -> dict[str, str]:
    stamp = envelope["event_id"]
    safe_patient = re.sub(r"[^\w.-]+", "_", parsed["patient"] or "unknown")

    bronze_record = {**envelope, "landed_at": datetime.now(timezone.utc).isoformat(), "layer": "bronze"}
    if STRIP_PAYLOAD_FROM_BRONZE:
        bronze_record.pop("payload", None)
        bronze_record["payload_omitted"] = True
        bronze_record["payload_bytes"] = len(str(envelope.get("payload") or ""))

    bronze_path = PATHS["bronze"] / f"{stamp}.json"
    write_json(bronze_path, bronze_record)

    payload = envelope.get("payload")
    if isinstance(payload, dict):
        payload_text = json.dumps(payload, indent=2)
    else:
        payload_text = str(payload or "")

    volume_path = PATHS["volume"] / f"{safe_patient}_{stamp}.{ext}"
    volume_path.write_text(payload_text, encoding="utf-8")

    silver_dir = PATHS["silver"] / safe_patient
    silver_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        silver_dir / f"{stamp}_summary.json",
        {
            "event_id": envelope["event_id"],
            "trace_id": envelope["trace_id"],
            "patient_key": envelope["patient_key"],
            **parsed["silver"],
            "volume_path": str(volume_path),
            "bronze_path": str(bronze_path),
        },
    )

    for chunk in parsed["chunks"]:
        if chunk["kind"] == "xml-overview":
            name = f"{stamp}_overview.md"
        else:
            title = re.sub(r"[^\w.-]+", "_", (chunk.get("title") or "section"))[:40]
            name = f"{stamp}_{title}.md"
        (silver_dir / name).write_text(chunk["content"], encoding="utf-8")

    return {
        "bronze_path": str(bronze_path),
        "volume_path": str(volume_path),
        "silver_dir": str(silver_dir),
    }


def land_dlq(envelope: dict[str, Any] | None, reason: str, errors: list[str]) -> str:
    stamp = (envelope or {}).get("event_id") or f"unknown_{int(datetime.now().timestamp())}"
    dlq_path = PATHS["dlq"] / f"{stamp}.json"
    write_json(
        dlq_path,
        {
            "reason": reason,
            "errors": errors,
            "envelope": envelope,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    return str(dlq_path)


def produce_json(producer: Producer, topic: str, key: str | None, value: dict[str, Any]) -> None:
    producer.produce(
        topic,
        key=(key or "").encode("utf-8") if key is not None else None,
        value=json.dumps(value).encode("utf-8"),
    )


def route_feed(topic: str, envelope: dict[str, Any]) -> str:
    if topic == TOPICS["hl7_adt_raw"] or envelope.get("feed") == "hl7_adt":
        return "hl7"
    if topic == TOPICS["fhir_raw"] or envelope.get("feed") == "fhir":
        return "fhir"
    return "ccda"


def handle_message(producer: Producer, raw: str, key: str | None, topic: str) -> None:
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError:
        fake = {
            "event_id": f"parse_error_{int(datetime.now().timestamp())}",
            "trace_id": None,
            "patient_key": key or "unknown",
            "payload": raw[:2000],
        }
        dlq_path = land_dlq(fake, "invalid_json", ["Message is not valid JSON"])
        produce_json(
            producer,
            TOPICS["dlq"],
            fake["patient_key"],
            {**fake, "reason": "invalid_json", "dlq_path": dlq_path},
        )
        producer.flush(5)
        print(f"[DLQ] invalid JSON from {topic}")
        return

    schema_errors = validate_local(envelope, "envelope.schema.json")
    if schema_errors:
        dlq_path = land_dlq(envelope, "schema_failed", schema_errors)
        produce_json(
            producer,
            TOPICS["dlq"],
            envelope.get("patient_key"),
            {
                "event_type": "healthcare.dlq",
                "reason": "schema_failed",
                "errors": schema_errors,
                "original_event_id": envelope.get("event_id"),
                "dlq_path": dlq_path,
            },
        )
        producer.flush(5)
        print(safe_log(f"[DLQ/schema] {envelope.get('event_id')}: {schema_errors[0]}"))
        return

    feed = route_feed(topic, envelope)
    if feed == "hl7":
        validation = validate_hl7_envelope(envelope)
    elif feed == "fhir":
        validation = validate_fhir_envelope(envelope)
    else:
        validation = validate_ccda_envelope(envelope)

    if not validation["ok"]:
        dlq_path = land_dlq(envelope, "validation_failed", validation["errors"])
        produce_json(
            producer,
            TOPICS["dlq"],
            envelope.get("patient_key"),
            {
                "event_type": "healthcare.dlq",
                "reason": "validation_failed",
                "feed": feed,
                "errors": validation["errors"],
                "original_event_id": envelope.get("event_id"),
                "patient_key": envelope.get("patient_key"),
                "dlq_path": dlq_path,
            },
        )
        produce_json(
            producer,
            TOPICS["audit"],
            envelope.get("event_id"),
            build_audit_event(
                action="dlq",
                event_id=envelope.get("event_id"),
                trace_id=envelope.get("trace_id"),
                topic=topic,
                detail="; ".join(validation["errors"]),
            ),
        )
        producer.flush(5)
        print(safe_log(f"[DLQ/{feed}] {envelope.get('event_id')}: {'; '.join(validation['errors'])}"))
        return

    try:
        source_name = envelope.get("source_file") or f"{envelope['event_id']}"
        if feed == "hl7":
            parsed = parse_hl7_adt(envelope["payload"], source_name)
            ext = "hl7"
            event_type = "clinical.adt.normalized"
        elif feed == "fhir":
            parsed = parse_fhir_resource(envelope["payload"], source_name)
            ext = "json"
            event_type = "clinical.fhir.normalized"
        else:
            parsed = parse_ccda_xml(envelope["payload"], source_name)
            ext = "xml"
            event_type = "clinical.document.normalized"

        landed = land_success(envelope, parsed, ext)
        normalized = {
            "event_id": envelope["event_id"],
            "trace_id": envelope["trace_id"],
            "event_type": event_type,
            "feed": feed,
            "patient_key": envelope["patient_key"],
            "patient": parsed["patient"],
            "section_count": parsed["silver"].get("section_count"),
            "section_titles": parsed["silver"].get("section_titles"),
            "volume_path": landed["volume_path"],
            "silver_dir": landed["silver_dir"],
            "bronze_path": landed["bronze_path"],
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "details": {
                k: parsed["silver"].get(k)
                for k in (
                    "hl7_trigger",
                    "hl7_event_name",
                    "location",
                    "patient_class",
                    "resource_types",
                    "observation_summaries",
                    "encounter_summaries",
                )
                if parsed["silver"].get(k) is not None
            },
        }
        norm_errors = validate_local(normalized, "normalized.schema.json")
        if norm_errors:
            raise ValueError(f"normalized schema failed: {norm_errors[0]}")

        produce_json(producer, TOPICS["clinical_normalized"], envelope["patient_key"], normalized)
        produce_json(
            producer,
            TOPICS["audit"],
            envelope["event_id"],
            build_audit_event(
                action="normalized",
                event_id=envelope["event_id"],
                trace_id=envelope["trace_id"],
                topic=topic,
                detail=f"feed={feed}; patient_key={envelope['patient_key']}",
            ),
        )
        producer.flush(5)
        print(safe_log(f"[OK/{feed}] {json.dumps(redact_envelope_for_log(envelope))}"))
        if validation["warnings"]:
            print(safe_log(f"     warnings: {'; '.join(validation['warnings'])}"))
    except Exception as exc:  # noqa: BLE001
        dlq_path = land_dlq(envelope, "parse_failed", [str(exc)])
        produce_json(
            producer,
            TOPICS["dlq"],
            envelope.get("patient_key"),
            {
                "event_type": "healthcare.dlq",
                "reason": "parse_failed",
                "feed": feed,
                "error": str(exc),
                "original_event_id": envelope.get("event_id"),
                "dlq_path": dlq_path,
            },
        )
        producer.flush(5)
        print(safe_log(f"[DLQ/{feed}] parse failed {envelope.get('event_id')}: {exc}"))


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    ensure_dirs()

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": "healthcare-multiformat-workers-python",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "client.id": "healthcare-worker-py",
        }
    )
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS, "client.id": "healthcare-worker-producer"})
    consumer.subscribe(RAW_TOPICS)

    print("Healthcare multi-format worker started (C-CDA + HL7 ADT + FHIR)")
    print(f"  consuming: {', '.join(RAW_TOPICS)}")
    print(f"  landing:   {PATHS['bronze']}")
    print("  Ctrl+C to stop\n")

    while RUNNING:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {msg.error()}")
            continue
        key = msg.key().decode("utf-8") if msg.key() else None
        raw = msg.value().decode("utf-8") if msg.value() else ""
        handle_message(producer, raw, key, msg.topic())

    consumer.close()
    producer.flush(5)
    print("Worker stopped.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
