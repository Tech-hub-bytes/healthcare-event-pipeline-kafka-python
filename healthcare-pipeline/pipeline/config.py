from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
CLIENT_ID = "healthcare-pipeline-python"

TOPICS = {
    "ccda_raw": "ccda.documents.raw",
    "hl7_adt_raw": "hl7.adt.raw",
    "fhir_raw": "fhir.resources.raw",
    "clinical_normalized": "clinical.events.normalized",
    "dlq": "healthcare.dlq",
    "audit": "healthcare.audit",
}

# Topics the multi-format worker consumes
RAW_TOPICS = [
    TOPICS["ccda_raw"],
    TOPICS["hl7_adt_raw"],
    TOPICS["fhir_raw"],
]

PATHS = {
    "bronze": ROOT / "landing" / "bronze",
    "silver": ROOT / "landing" / "silver",
    "volume": ROOT / "landing" / "volume",
    "dlq": ROOT / "landing" / "dlq",
    "audit": ROOT / "landing" / "audit",
    "samples": ROOT / "samples",
}
