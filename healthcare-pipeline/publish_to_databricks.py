from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import PATHS, ROOT

PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE", "dbc-7c3eed4c")
VOLUME_ROOT = os.getenv(
    "CCDA_VOLUME_URI",
    "dbfs:/Volumes/workspace/default/ccda_chatbot_docs",
)
# Also publish into the running chatbot app volume when set (comma-separated)
EXTRA_VOLUMES = [
    v.strip()
    for v in os.getenv(
        "CCDA_EXTRA_VOLUME_URIS",
        "dbfs:/Volumes/workspace/ccda_rag/docs",
    ).split(",")
    if v.strip()
]
PIPELINE_PREFIX = f"{VOLUME_ROOT}/kafka_pipeline"
CATALOG_SCHEMA = os.getenv("CCDA_UC_SCHEMA", "workspace.default")
EVENTS_TABLE = f"{CATALOG_SCHEMA}.ccda_pipeline_events"


def dbx(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = run(["databricks", *args, "--profile", PROFILE], check=False)
    if check and proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(err or f"databricks {' '.join(args)} failed")
    return proc


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(cmd))
    proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "").strip())
    return proc


def ensure_dirs() -> None:
    for suffix in ("xml", "silver", "bronze", "from_kafka"):
        uri = f"{PIPELINE_PREFIX}/{suffix}"
        dbx(["fs", "mkdirs", uri], check=False)


def upload_file(local: Path, remote_uri: str) -> None:
    # Ensure parent directory exists on the volume (dbfs cp won't create nested dirs)
    parent = remote_uri.rsplit("/", 1)[0]
    dbx(["fs", "mkdirs", parent], check=False)
    dbx(["fs", "cp", str(local), remote_uri, "--overwrite"])


def upload_xmls() -> list[Path]:
    files = sorted(PATHS["volume"].glob("*.xml"))
    targets = [PIPELINE_PREFIX, *[f"{v.rstrip('/')}/kafka_pipeline" for v in EXTRA_VOLUMES]]
    for prefix in targets:
        for f in files:
            upload_file(f, f"{prefix}/xml/{f.name}")
            upload_file(f, f"{prefix}/from_kafka/{f.name}")
            # Root copy helps apps that only scan shallow paths
            if prefix.endswith("/kafka_pipeline"):
                root = prefix[: -len("/kafka_pipeline")]
                upload_file(f, f"{root}/{f.name}")
    return files


def upload_silver() -> list[Path]:
    summaries = sorted(PATHS["silver"].rglob("*_summary.json"))
    for summary in summaries:
        rel = summary.relative_to(PATHS["silver"]).as_posix()
        upload_file(summary, f"{PIPELINE_PREFIX}/silver/{rel}")
        prefix = summary.name.replace("_summary.json", "")
        for md in summary.parent.glob(f"{prefix}*.md"):
            md_rel = md.relative_to(PATHS["silver"]).as_posix()
            upload_file(md, f"{PIPELINE_PREFIX}/silver/{md_rel}")
    return summaries


def upload_bronze() -> list[Path]:
    files = sorted(PATHS["bronze"].glob("*.json"))
    for f in files:
        # Strip large payload before UC upload for bronze index (keep event metadata)
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            upload_file(f, f"{PIPELINE_PREFIX}/bronze/{f.name}")
            continue
        slim = {k: v for k, v in data.items() if k != "payload"}
        slim["payload_omitted"] = True
        tmp = PATHS["bronze"] / f"_slim_{f.name}"
        tmp.write_text(json.dumps(slim, indent=2), encoding="utf-8")
        upload_file(tmp, f"{PIPELINE_PREFIX}/bronze/{f.name}")
        tmp.unlink(missing_ok=True)
    return files


def sql(query: str) -> None:
    # Use CLI experimental query tool
    cmd = [
        "databricks",
        "experimental",
        "aitools",
        "tools",
        "query",
        query,
        "--profile",
        PROFILE,
    ]
    print("+", " ".join(cmd[:6]), "<sql>")
    proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout)
        raise RuntimeError(f"SQL failed: {proc.stderr or proc.stdout}")
    if proc.stdout.strip():
        print(proc.stdout.strip()[:1000])


def ensure_delta_table() -> None:
    sql(
        f"""
CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
  event_id STRING,
  trace_id STRING,
  patient_key STRING,
  patient STRING,
  gender STRING,
  birth_time STRING,
  document_title STRING,
  section_count INT,
  section_titles STRING,
  volume_file STRING,
  parsed_at STRING,
  ingested_at TIMESTAMP
) USING DELTA
"""
    )


def merge_summaries(summaries: list[Path]) -> int:
    if not summaries:
        return 0
    ensure_delta_table()
    count = 0
    for summary in summaries:
        data = json.loads(summary.read_text(encoding="utf-8"))
        event_id = data.get("event_id")
        if not event_id:
            continue
        titles = json.dumps(data.get("section_titles") or [])
        volume_file = Path(data.get("volume_path") or "").name
        # Escape single quotes for SQL literals
        def q(v: object) -> str:
            if v is None:
                return "NULL"
            s = str(v).replace("'", "''")
            return f"'{s}'"

        stmt = f"""
MERGE INTO {EVENTS_TABLE} AS t
USING (
  SELECT
    {q(event_id)} AS event_id,
    {q(data.get('trace_id'))} AS trace_id,
    {q(data.get('patient_key'))} AS patient_key,
    {q(data.get('patient'))} AS patient,
    {q(data.get('gender'))} AS gender,
    {q(data.get('birth_time'))} AS birth_time,
    {q(data.get('document_title'))} AS document_title,
    {int(data.get('section_count') or 0)} AS section_count,
    {q(titles)} AS section_titles,
    {q(volume_file)} AS volume_file,
    {q(data.get('parsed_at'))} AS parsed_at,
    current_timestamp() AS ingested_at
) AS s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET
  t.trace_id = s.trace_id,
  t.patient_key = s.patient_key,
  t.patient = s.patient,
  t.gender = s.gender,
  t.birth_time = s.birth_time,
  t.document_title = s.document_title,
  t.section_count = s.section_count,
  t.section_titles = s.section_titles,
  t.volume_file = s.volume_file,
  t.parsed_at = s.parsed_at,
  t.ingested_at = s.ingested_at
WHEN NOT MATCHED THEN INSERT (
  event_id, trace_id, patient_key, patient, gender, birth_time,
  document_title, section_count, section_titles, volume_file, parsed_at, ingested_at
) VALUES (
  s.event_id, s.trace_id, s.patient_key, s.patient, s.gender, s.birth_time,
  s.document_title, s.section_count, s.section_titles, s.volume_file, s.parsed_at, s.ingested_at
)
"""
        sql(stmt)
        count += 1
    return count


def write_manifest(xmls: list[Path], summaries: list[Path]) -> Path:
    manifest = {
        "published_at": datetime.now(timezone.utc).isoformat(),
        "profile": PROFILE,
        "volume": VOLUME_ROOT,
        "pipeline_prefix": PIPELINE_PREFIX,
        "events_table": EVENTS_TABLE,
        "xml_count": len(xmls),
        "summary_count": len(summaries),
        "xml_files": [f.name for f in xmls],
        "summary_files": [s.name for s in summaries],
    }
    out = ROOT / "landing" / "last_publish.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    upload_file(out, f"{PIPELINE_PREFIX}/last_publish.json")
    return out


def main() -> None:
    print(f"Publishing healthcare pipeline landing zone to Databricks")
    print(f"  profile: {PROFILE}")
    print(f"  volume:  {VOLUME_ROOT}")
    print(f"  table:   {EVENTS_TABLE}\n")

    ensure_dirs()
    xmls = upload_xmls()
    print(f"Uploaded {len(xmls)} XML file(s)")
    summaries = upload_silver()
    print(f"Uploaded {len(summaries)} silver summary file(s)")
    bronze = upload_bronze()
    print(f"Uploaded {len(bronze)} bronze metadata file(s)")
    merged = merge_summaries(summaries)
    print(f"Merged {merged} row(s) into {EVENTS_TABLE}")
    manifest = write_manifest(xmls, summaries)
    print(f"\nPublish complete. Manifest: {manifest}")
    print(f"UC path: {PIPELINE_PREFIX}/from_kafka/")
    print("Next: python refresh_chatbot.py   (optional chatbot re-index)")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(str(exc), file=sys.stderr)
        sys.exit(1)
