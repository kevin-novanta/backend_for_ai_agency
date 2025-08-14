from __future__ import annotations
from datetime import datetime, time
from pathlib import Path
import json
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
import re, os

CONTROLS_PATH = Path("/Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/utils/followup_controls.json")
COUNTERS_PATH = Path(__file__).parent / "send_counters.json"

DAYS_MAP = {
    "Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6,
    "Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
}

def _load_controls() -> dict:
    if not CONTROLS_PATH.exists():
        # sane defaults if controls missing
        return {
            "outreach_enabled": True,
            "start_time": "09:00",
            "end_time": "17:00",
            "days_allowed": ["Mon", "Tue", "Wed", "Thu", "Fri"],
            "daily_limit": 200,
            "per_inbox_limit": 40,
            "timezone": "America/New_York",
        }
    with open(CONTROLS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("timezone", "America/New_York")
    return data

def _parse_hhmm(s: str) -> time:
    val = (s or "").strip()
    if not re.match(r"^\d{1,2}:\d{2}$", val):
        raise ValueError("expected HH:MM 24-hour")
    hh_s, mm_s = val.split(":", 1)
    hh, mm = int(hh_s), int(mm_s)
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError("hour/minute out of range")
    return time(hh, mm)

def _now_local(tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz)

def _load_counters(today: str) -> dict:
    if COUNTERS_PATH.exists():
        try:
            with open(COUNTERS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}
    else:
        data = {}
    if data.get("date") != today:
        data = {"date": today, "total": 0, "per_inbox": {}}
    data.setdefault("total", 0)
    data.setdefault("per_inbox", {})
    return data

def _save_counters(data: dict) -> None:
    with open(COUNTERS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def check_send_window(*, inbox: Optional[str] = None, dry_run: bool = True, bypass_time: bool = False) -> Tuple[bool, str]:
    """Return (allowed, reason). If allowed and not dry_run, increments counters.
    Reasons: 'disabled', 'day', 'time', 'daily_limit', 'per_inbox_limit', 'ok'.
    """
    # Allow bypass via arg or env var (SEQ_BYPASS_TIME=1)
    if not bypass_time:
        bypass_time = os.environ.get("SEQ_BYPASS_TIME") == "1"

    cfg = _load_controls()
    if not cfg.get("outreach_enabled", True):
        return False, "disabled"

    tz_name = cfg.get("timezone", "America/New_York")
    now = _now_local(tz_name)
    today = now.date().isoformat()
    weekday = now.weekday()

    allowed_days = cfg.get("days_allowed", ["Mon", "Tue", "Wed", "Thu", "Fri"])
    allowed_idx = {DAYS_MAP.get(d, -1) for d in allowed_days}
    if weekday not in allowed_idx:
        return False, "day"

    if not bypass_time:
        try:
            start_t = _parse_hhmm(cfg.get("start_time", "09:00"))
            end_t = _parse_hhmm(cfg.get("end_time", "17:00"))
        except Exception:
            return False, "time"
        cur_t = now.timetz().replace(tzinfo=None)
        if not (start_t <= cur_t <= end_t):
            return False, "time"

    counters = _load_counters(today)
    daily_limit = int(cfg.get("daily_limit", 999999))
    if int(counters.get("total", 0)) >= daily_limit:
        return False, "daily_limit"

    per_inbox_limit = cfg.get("per_inbox_limit")
    if per_inbox_limit is not None and inbox:
        per_inbox = counters.get("per_inbox", {})
        if int(per_inbox.get(inbox, 0)) >= int(per_inbox_limit):
            return False, "per_inbox_limit"

    if not dry_run:
        counters["total"] = int(counters.get("total", 0)) + 1
        if inbox:
            counters.setdefault("per_inbox", {})
            counters["per_inbox"][inbox] = int(counters["per_inbox"].get(inbox, 0)) + 1
        _save_counters(counters)
    return True, "ok"
