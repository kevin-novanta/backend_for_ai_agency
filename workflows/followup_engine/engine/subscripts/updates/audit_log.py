from __future__ import annotations
from pathlib import Path
from datetime import datetime
import json
from typing import Any, Optional

__all__ = ["log_action"]

# followup_engine root (…/workflows/followup_engine)
_ROOT = Path(__file__).resolve().parents[3]
_LOG_DIR = _ROOT / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)

def _log_path() -> Path:
    # single rolling file (switch to timestamped if you prefer)
    return _LOG_DIR / "followup_run.log"

def log_action(
    *,
    client: Optional[str],
    lead: Optional[str],
    followup: Optional[int],
    inbox: Optional[str],
    result: dict[str, Any]
) -> None:
    """
    Append a structured JSON line to logs/followup_run.log and print a short line to console.
    """
    ts = datetime.utcnow().isoformat() + "Z"
    payload = {
        "ts": ts,
        "client": client,
        "lead": lead,
        "followup": followup,
        "inbox": inbox,
        "result": result,
    }
    try:
        path = _log_path()
        print(f"[audit_log] Opening file: {path}")
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # Never let logging crash the run
        pass

    # Console echo
    status = result.get("status") if isinstance(result, dict) else None
    reason = result.get("reason") if isinstance(result, dict) else None
    thread = result.get("thread_link") if isinstance(result, dict) else None
    print(f"[{ts}] lead={lead} fu={followup} inbox={inbox} status={status} reason={reason} thread={thread}")