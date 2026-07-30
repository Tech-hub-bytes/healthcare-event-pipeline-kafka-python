from __future__ import annotations

import os
from pathlib import Path

# Security / PHI controls
SECURITY_MODE = os.getenv("PIPELINE_SECURITY_MODE", "strict")  # off | strict
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8083")
ENFORCE_JSON_SCHEMA = os.getenv("ENFORCE_JSON_SCHEMA", "1") == "1"
STRIP_PAYLOAD_FROM_BRONZE = os.getenv("STRIP_PAYLOAD_FROM_BRONZE", "1") == "1"
REDACT_PHI_IN_LOGS = os.getenv("REDACT_PHI_IN_LOGS", "1") == "1"
ALLOW_PATIENT_DISPLAY_IN_LOGS = os.getenv("ALLOW_PATIENT_DISPLAY_IN_LOGS", "0") == "1"

# Retention (days) — apply via retention_job.py
RETENTION_DAYS = {
    "bronze": int(os.getenv("RETENTION_BRONZE_DAYS", "7")),
    "dlq": int(os.getenv("RETENTION_DLQ_DAYS", "30")),
    "audit": int(os.getenv("RETENTION_AUDIT_DAYS", "90")),
    "volume": int(os.getenv("RETENTION_VOLUME_DAYS", "365")),
    "silver": int(os.getenv("RETENTION_SILVER_DAYS", "365")),
    "metrics": int(os.getenv("RETENTION_METRICS_DAYS", "14")),
}

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
