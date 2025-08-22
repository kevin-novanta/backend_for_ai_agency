# /Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/engine/subscripts/gating/thread_guard.py
from __future__ import annotations
from typing import Dict, Any, Tuple
from pathlib import Path
import json

from engine.subscripts.utils.crm_helpers import get, setf
from engine.subscripts.io.thread_resolver import find_thread_by_signals  # NEW

__all__ = ["require_thread_link", "thread_guard"]

def _load_settings(SETTINGS_DIR: Path) -> dict:
    # Try several casings just like main.py does
    for name in ["thread_resolver.json", "Thread_Resolver.json", "Thread_Resolver.JSON", "thread_resolver.JSON"]:
        p = SETTINGS_DIR / name
        if p.exists():
            return json.loads(p.read_text())
    # Sensible defaults if settings not found
    return {
        "enabled": True,
        "max_candidates": 10,
        "use_subject": True,
        "use_body_fingerprint": True,
        "use_opener_date": True
    }

def require_thread_link(
    row: Dict[str, Any],
    fields_map: Dict[str, Any],
    *,
    inbox: str | None = None,
    dry_run: bool = False,
    settings_dir: Path | None = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Guard: ensure we have a thread link; try recovery if missing.
    Returns (ok, info). info always safe for audit logs.
    """
    print("[thread_guard] Checking thread link requirement...")
    can = fields_map.get("canonical", {})
    thread_col = can.get("thread_link", "Email Thread Link")
    link = (get(row, thread_col) or "").strip()
    print(f"[thread_guard] Using column '{thread_col}', value: '{link}'")

    if link:
        print(f"[thread_guard] Allowed: existing thread link found → {link}")
        return True, {"status": "ok", "thread_link": link, "recovered": False}

    # No link → try resolver (if enabled and we know the inbox)
    if inbox:
        settings = _load_settings(settings_dir or (Path(__file__).resolve().parents[3] / "settings"))
        if settings.get("enabled", True):
            print("[thread_guard] No thread link; attempting Gmail thread recovery...")
            recovered = find_thread_by_signals(row, fields_map, inbox, max_candidates=settings.get("max_candidates", 10))
            if recovered and recovered.get("thread_link"):
                link = recovered["thread_link"]
                print(f"[thread_guard] Recovered thread: {link}")
                # Write back to the row so caller can persist to CSV if not dry-run
                setf(row, thread_col, link)
                return True, {"status": "ok", "thread_link": link, "recovered": True}

    reason = {"status": "skip", "reason": "no_thread_link"}
    print("[thread_guard] Blocked: no thread link present and recovery failed; skipping lead.")
    return False, reason

# Alias for backward compatibility with main.py imports
thread_guard = require_thread_link