from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable


def run(script: str, args: list[str] | None = None) -> None:
    cmd = [PY, str(ROOT / script), *(args or [])]
    subprocess.check_call(cmd, cwd=str(ROOT))


def main() -> None:
    print("=== Healthcare Kafka pipeline demo (Python) ===\n")
    print("1) Creating topics...")
    run("create_topics.py")

    print("\n2) Starting worker (background)...")
    worker = subprocess.Popen(
        [PY, str(ROOT / "worker.py")],
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    time.sleep(3)

    print("\n3) Producing valid C-CDA sample...")
    run("produce_ccda.py", [str(ROOT / "samples" / "emma-ccda.xml")])

    print("\n4) Producing invalid sample (should go to DLQ)...")
    run("produce_ccda.py", [str(ROOT / "samples" / "invalid-sample.xml")])

    print("\n5) Waiting for worker to process...")
    time.sleep(6)

    # Drain some worker output
    try:
        worker.terminate()
        out, _ = worker.communicate(timeout=5)
        if out:
            print("\n--- worker output ---")
            print(out)
    except Exception:  # noqa: BLE001
        worker.kill()

    print("\n=== Demo complete ===")
    print(f"Bronze:  {ROOT / 'landing' / 'bronze'}")
    print(f"Silver:  {ROOT / 'landing' / 'silver'}")
    print(f"Volume:  {ROOT / 'landing' / 'volume'}")
    print(f"DLQ:     {ROOT / 'landing' / 'dlq'}")
    print("Kafka UI: http://localhost:8088")
    print("\nTo keep processing:  python worker.py")
    print("To send more files:  python produce_ccda.py path\\to\\file.xml")


if __name__ == "__main__":
    main()
