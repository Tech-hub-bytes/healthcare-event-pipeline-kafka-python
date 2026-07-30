from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable


def run(script: str, args: list[str] | None = None) -> None:
    subprocess.check_call([PY, str(ROOT / script), *(args or [])], cwd=str(ROOT))


def main() -> None:
    print("=== P4 multi-format clinical feeds demo ===\n")
    print("1) Topics...")
    run("create_topics.py")

    print("\n2) Start worker...")
    worker = subprocess.Popen(
        [PY, str(ROOT / "worker.py")],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    time.sleep(3)

    print("\n3) Produce HL7 ADT A01 + A03...")
    run("produce_hl7.py", [str(ROOT / "samples" / "emma-adt-a01.hl7")])
    run("produce_hl7.py", [str(ROOT / "samples" / "emma-adt-a03.hl7")])

    print("\n4) Produce FHIR bundle...")
    run("produce_fhir.py", [str(ROOT / "samples" / "emma-fhir-bundle.json")])

    print("\n5) Produce invalid HL7 + FHIR (DLQ)...")
    run("produce_hl7.py", [str(ROOT / "samples" / "invalid-hl7.hl7")])
    run("produce_fhir.py", [str(ROOT / "samples" / "invalid-fhir.json")])

    print("\n6) Wait for processing...")
    time.sleep(8)
    try:
        worker.terminate()
        out, _ = worker.communicate(timeout=5)
        print("\n--- worker output ---")
        print(out or "")
    except Exception:  # noqa: BLE001
        worker.kill()

    print("\n=== P4 demo complete ===")
    print(f"Silver: {ROOT / 'landing' / 'silver'}")
    print(f"Volume: {ROOT / 'landing' / 'volume'}")
    print(f"DLQ:    {ROOT / 'landing' / 'dlq'}")
    print("Kafka UI: http://localhost:8088")


if __name__ == "__main__":
    main()
