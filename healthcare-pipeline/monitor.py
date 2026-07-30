from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import request

from confluent_kafka.admin import AdminClient

from pipeline.config import KAFKA_BROKERS, PATHS, ROOT, TOPICS
from pipeline.hardening import RETENTION_DAYS, SCHEMA_REGISTRY_URL
from pipeline.schema_guard import schema_registry_up

METRICS_DIR = ROOT / "landing" / "metrics"


def _http_ok(url: str) -> bool:
    try:
        with request.urlopen(url, timeout=3) as resp:
            return 200 <= resp.status < 300
    except Exception:  # noqa: BLE001
        return False


def count_files(path: Path, pattern: str = "*") -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.rglob(pattern) if _.is_file())


def collect() -> dict[str, Any]:
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    md = admin.list_topics(timeout=10)
    topics = sorted(md.topics.keys())
    required = set(TOPICS.values())
    missing = sorted(required - set(topics))

    status = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "kafka_brokers": KAFKA_BROKERS,
        "kafka_ok": len(missing) == 0,
        "missing_topics": missing,
        "topic_count": len(topics),
        "schema_registry_url": SCHEMA_REGISTRY_URL,
        "schema_registry_ok": schema_registry_up(),
        "kafka_ui_ok": _http_ok("http://localhost:8088"),
        "landing": {
            "bronze_files": count_files(PATHS["bronze"], "*.json"),
            "silver_files": count_files(PATHS["silver"], "*.*"),
            "volume_files": count_files(PATHS["volume"], "*.*"),
            "dlq_files": count_files(PATHS["dlq"], "*.json"),
        },
        "alerts": [],
        "retention_days": RETENTION_DAYS,
    }

    if not status["kafka_ok"]:
        status["alerts"].append({"severity": "critical", "message": f"Missing topics: {missing}"})
    if not status["schema_registry_ok"]:
        status["alerts"].append(
            {"severity": "warning", "message": "Schema Registry offline (local JSON Schema still enforced)"}
        )
    if status["landing"]["dlq_files"] > 0:
        status["alerts"].append(
            {
                "severity": "warning",
                "message": f"DLQ has {status['landing']['dlq_files']} file(s) — review rejected messages",
            }
        )
    status["healthy"] = status["kafka_ok"] and len([a for a in status["alerts"] if a["severity"] == "critical"]) == 0
    return status


def main() -> None:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    status = collect()
    out = METRICS_DIR / "health.json"
    out.write_text(json.dumps(status, indent=2), encoding="utf-8")
    # append history point
    hist = METRICS_DIR / "health_history.ndjson"
    with hist.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"ts": time.time(), **status}) + "\n")

    print("P5 monitoring health check")
    print(f"  healthy:          {status['healthy']}")
    print(f"  kafka_ok:         {status['kafka_ok']}")
    print(f"  schema_registry:  {status['schema_registry_ok']}")
    print(f"  kafka_ui:         {status['kafka_ui_ok']}")
    print(f"  dlq_files:        {status['landing']['dlq_files']}")
    print(f"  bronze_files:     {status['landing']['bronze_files']}")
    for alert in status["alerts"]:
        print(f"  ! [{alert['severity']}] {alert['message']}")
    print(f"\nWrote {out}")
    raise SystemExit(0 if status["healthy"] else 2)


if __name__ == "__main__":
    main()
