from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import PATHS, ROOT
from pipeline.hardening import RETENTION_DAYS

METRICS_DIR = ROOT / "landing" / "metrics"


def _purge(folder: Path, days: int) -> int:
    if not folder.exists() or days <= 0:
        return 0
    cutoff = time.time() - days * 86400
    removed = 0
    for path in folder.rglob("*"):
        if path.is_file() and path.stat().st_mtime < cutoff:
            path.unlink(missing_ok=True)
            removed += 1
    return removed


def main() -> None:
    print("P5 retention job")
    print(f"  ran_at: {datetime.now(timezone.utc).isoformat()}")
    mapping = {
        "bronze": PATHS["bronze"],
        "dlq": PATHS["dlq"],
        "audit": PATHS["audit"],
        "volume": PATHS["volume"],
        "silver": PATHS["silver"],
        "metrics": METRICS_DIR,
    }
    total = 0
    for name, path in mapping.items():
        days = RETENTION_DAYS.get(name, 30)
        n = _purge(path, days)
        total += n
        print(f"  {name}: removed {n} file(s) older than {days} day(s)")
    print(f"Done. Removed {total} file(s).")


if __name__ == "__main__":
    main()
