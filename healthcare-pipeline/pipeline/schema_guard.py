from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib import error, request

from pipeline.hardening import (
    ENFORCE_JSON_SCHEMA,
    SCHEMA_REGISTRY_URL,
    SCHEMAS_DIR,
    SECURITY_MODE,
)

try:
    import jsonschema
    from jsonschema import Draft202012Validator
except ImportError:  # pragma: no cover
    jsonschema = None
    Draft202012Validator = None


def load_schema(name: str) -> dict[str, Any]:
    path = SCHEMAS_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


def validate_local(instance: dict[str, Any], schema_name: str) -> list[str]:
    if not ENFORCE_JSON_SCHEMA or SECURITY_MODE == "off":
        return []
    if jsonschema is None:
        return ["jsonschema package not installed"]
    schema = load_schema(schema_name)
    validator = Draft202012Validator(schema)
    return [e.message for e in sorted(validator.iter_errors(instance), key=lambda e: list(e.path))]


def register_schema(subject: str, schema_obj: dict[str, Any]) -> dict[str, Any]:
    """Register JSON Schema with Confluent Schema Registry (optional)."""
    body = json.dumps(
        {
            "schemaType": "JSON",
            "schema": json.dumps(schema_obj),
        }
    ).encode("utf-8")
    req = request.Request(
        f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects/{subject}/versions",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Schema registry HTTP {exc.code}: {detail}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Schema registry unavailable: {exc}") from exc


def schema_registry_up() -> bool:
    try:
        with request.urlopen(f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects", timeout=3) as resp:
            return 200 <= resp.status < 300
    except Exception:  # noqa: BLE001
        return False


def ensure_registered() -> list[str]:
    messages = []
    if not schema_registry_up():
        return ["schema-registry offline — local JSON Schema validation still active"]
    mapping = {
        "healthcare-envelope-value": "envelope.schema.json",
        "clinical-normalized-value": "normalized.schema.json",
    }
    for subject, filename in mapping.items():
        result = register_schema(subject, load_schema(filename))
        messages.append(f"registered {subject} id={result.get('id')}")
    return messages
