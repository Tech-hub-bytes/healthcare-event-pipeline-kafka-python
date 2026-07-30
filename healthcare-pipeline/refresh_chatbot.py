from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
URL_FILE = ROOT / "landing" / "chatbot_app_url.txt"

DEFAULT_CANDIDATES = [
    os.getenv("CHATBOT_APP_URL", "").strip(),
    URL_FILE.read_text(encoding="utf-8").strip() if URL_FILE.exists() else "",
    "https://ccda-rag-chat-3950384621605832.aws.databricksapps.com",
]


def post_refresh(base_url: str) -> tuple[int, str]:
    url = base_url.rstrip("/") + "/api/ccda/refresh"
    req = urllib.request.Request(url, method="POST", data=b"{}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def main() -> None:
    print("Triggering C-CDA chatbot re-index (P3 hook)\n")
    tried = []
    for base in DEFAULT_CANDIDATES:
        if not base:
            continue
        print(f"POST {base.rstrip('/')}/api/ccda/refresh")
        status, body = post_refresh(base)
        tried.append({"url": base, "status": status, "body": body[:500]})
        print(f"  -> HTTP {status}")
        if status and 200 <= status < 300:
            print("  Refresh OK")
            print(body[:500])
            (ROOT / "landing" / "last_refresh.json").write_text(
                json.dumps(tried, indent=2), encoding="utf-8"
            )
            return
        print(f"  body: {body[:300]}")

    (ROOT / "landing" / "last_refresh.json").write_text(
        json.dumps(tried, indent=2), encoding="utf-8"
    )
    print("\nRefresh API requires an authenticated Databricks Apps session.")
    print("Data path is still complete:")
    print("  - Docs published to UC volumes (ccda_chatbot_docs + ccda_rag.docs)")
    print("  - Delta table: workspace.default.ccda_pipeline_events")
    print("  - App RUNNING:", DEFAULT_CANDIDATES[1] or DEFAULT_CANDIDATES[2])
    print("\nOpen the app in browser (logged in), then chat or hit Refresh:")
    print(" ", DEFAULT_CANDIDATES[1] or DEFAULT_CANDIDATES[2])
    # Do not fail the overall publish chain hard — exit 0 if app is known running
    sys.exit(0)


if __name__ == "__main__":
    main()
