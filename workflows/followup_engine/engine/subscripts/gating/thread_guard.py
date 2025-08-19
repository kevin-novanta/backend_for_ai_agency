# /Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/engine/subscripts/gating/thread_guard.py
from __future__ import annotations
from typing import Dict, Any, Tuple
from engine.subscripts.utils.crm_helpers import get

__all__ = ["require_thread_link"]

def require_thread_link(row: Dict[str, Any], fields_map: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Guard: require an existing opener thread link before sending follow-ups.
    Returns (ok, info). If ok is False, caller should skip this lead.
    `info` is suitable for audit logging.
    """
    print("[thread_guard] Checking thread link requirement...")
    can = fields_map.get("canonical", {})
    thread_col = can.get("thread_link", "Email Thread Link")
    link = (get(row, thread_col) or "").strip()
    print(f"[thread_guard] Using column '{thread_col}', value: '{link}'")

    if not link:
        reason = {"status": "skip", "reason": "no_thread_link"}
        print("[thread_guard] Blocked: no thread link present; skipping lead.")
        return False, reason

    print(f"[thread_guard] Allowed: existing thread link found → {link}")
    return True, {"status": "ok", "thread_link": link}