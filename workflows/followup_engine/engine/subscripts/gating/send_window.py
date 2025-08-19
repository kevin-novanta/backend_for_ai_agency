from datetime import datetime
import json
import os

SEND_WINDOW_PATH = "/Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/engine/settings/send_window.json"

def load_send_window() -> dict:
    print("Loading send window configuration...")
    with open(SEND_WINDOW_PATH, "r") as f:
        config = json.load(f)
    print("Send window configuration loaded.")
    return config

def allowed_now(window_cfg: dict) -> tuple[bool, str]:
    """Check if sending is allowed right now based on send_window.json.
    Returns (ok, reason).
    """
    if not window_cfg:
        window_cfg = load_send_window()

    now = datetime.now()
    days_map = window_cfg.get("days_map", {})
    day = days_map.get(str(now.weekday()))
    print(f"Checking weekday mapping for weekday {now.weekday()}: mapped to {day}")
    if day is None:
        print(f"Blocked: day mapping missing for weekday {now.weekday()}")
        return False, f"day_mapping_missing:{now.weekday()}"

    hours = window_cfg.get("hours", {})
    start = hours["start"]
    end = hours["end"]
    days_allowed = window_cfg["days_allowed"]

    print(f"Checking if day '{day}' is in allowed days {days_allowed}")
    if day not in days_allowed:
        print(f"Blocked: day '{day}' is not allowed")
        return False, f"day_blocked:{day}"

    print(f"Checking if current hour {now.hour} is between {start} and {end}")
    if not (start <= now.hour < end):
        print(f"Blocked: hour {now.hour} is outside allowed range")
        return False, f"hour_blocked:{now.hour}"

    print("Allowed: current time is within allowed send window")
    return True, "ok"