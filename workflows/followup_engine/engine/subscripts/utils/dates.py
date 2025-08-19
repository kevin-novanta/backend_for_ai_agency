# engine/subscripts/utils/dates.py
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# --- Public API expected by main.py ---
__all__ = ["now_iso", "delay_ok"]

def now_iso() -> str:
    """Return current UTC time in ISO-8601 with 'Z' suffix."""
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# --- Helpers ---

_COMMON_PATTERNS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
]

def _parse_dt(s: str) -> Optional[datetime]:
    """Best-effort parse of a timestamp string; returns naive UTC-like datetime."""
    if not s:
        return None
    s = str(s).strip()
    # Normalize a trailing Z to something strptime can handle in some patterns
    for fmt in _COMMON_PATTERNS:
        try:
            dt = datetime.strptime(s, fmt)
            # Drop tzinfo to keep naive (we treat values as UTC-ish)
            return dt.replace(tzinfo=None)
        except Exception:
            continue
    # Fallback: try to trim trailing Z and reparse
    if s.endswith("Z"):
        try:
            return datetime.strptime(s[:-1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=None)
        except Exception:
            pass
    return None

def _required_wait_days(delays_cfg: Dict[str, Any], seq_stage: str) -> Optional[int]:
    """Look up required wait in days for the *current* stage before sending the next touch."""
    if not delays_cfg or not seq_stage:
        return None
    cfg = delays_cfg.get(seq_stage)
    if isinstance(cfg, dict):
        days = cfg.get("days")
        try:
            return int(days) if days is not None else None
        except Exception:
            return None
    try:
        return int(cfg)
    except Exception:
        return None

def delay_ok(delays_cfg: Dict[str, Any], seq_stage: str, last_sent_str: Optional[str]) -> bool:
    """
    True if enough time has elapsed since last_sent for the given seq_stage.
    - If no delay rule for stage → allow.
    - If no last_sent timestamp → allow (assume first send for this stage).
    - Otherwise require (now - last_sent) >= required days.
    """
    req_days = _required_wait_days(delays_cfg, seq_stage)
    if not req_days:
        return True

    last_dt = _parse_dt(last_sent_str) if last_sent_str else None
    if last_dt is None:
        # No recorded last sent → treat as eligible
        return True

    delta = datetime.utcnow() - last_dt
    return delta >= timedelta(days=req_days)