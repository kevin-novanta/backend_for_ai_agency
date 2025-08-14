#!/usr/bin/env python3
"""
Scratch test for gmail_watch pipeline (one-tick, no live Gmail).

What it does:
- Adds the repo root to sys.path so imports work when run directly
- Monkey-patches poll_inbox + classify_message to avoid Gmail
- Runs runner.run_once_for_inbox(inbox) and prints counters
- Optionally exercises resolve_lead + mark_yes/mark_no directly

Run:
    cd /Users/kevinnovanta/backend_for_ai_agency
    python3 workflows/followup_engine/gmail_watch/tests/test_gmail_watch.py

You can override test inputs via env vars:
    TEST_LEAD_EMAIL=someone@example.com TEST_INBOX=info@outbound-accelerator.com python3 .../test_gmail_watch.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict

# --- Put repo root on sys.path ---
THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[4]  # .../backend_for_ai_agency
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

print(f"[scratch] Using REPO_ROOT={REPO_ROOT}")

# --- Imports from the project ---
from workflows.followup_engine.gmail_watch.runtime.runner import run_once_for_inbox
from workflows.followup_engine.gmail_watch.Steps import poll_inbox, classify_message
from workflows.followup_engine.gmail_watch.Steps.resolve_lead import find_lead_row
from workflows.followup_engine.gmail_watch.Steps.mark_responded import mark_yes, mark_no

# --- Test inputs ---
TEST_LEAD_EMAIL = os.getenv("TEST_LEAD_EMAIL", "kevinbevans07@gmail.com")
TEST_INBOX = os.getenv("TEST_INBOX", "info@outbound-accelerator.com")
TEST_SUBJECT = os.getenv("TEST_SUBJECT", "Re: opener")
TEST_DATE_ISO = os.getenv("TEST_DATE_ISO", "2025-08-14T16:00:00Z")

print(f"[scratch] TEST_LEAD_EMAIL={TEST_LEAD_EMAIL}")
print(f"[scratch] TEST_INBOX={TEST_INBOX}")

# --- Monkey-patch poll + classify so we don't hit Gmail ---

def _fake_poll_ids(svc, inbox: str, since_ms: int, lookback_minutes: int = 1440):
    print(f"[scratch] _fake_poll_ids(inbox={inbox}, since_ms={since_ms}, lookback={lookback_minutes}) -> ['FAKE_MSG_ID']")
    return ["FAKE_MSG_ID"]


def _fake_classify(svc, msg_id: str, inbox: str) -> Dict:
    print(f"[scratch] _fake_classify(msg_id={msg_id}, inbox={inbox})")
    return {
        "from_email": TEST_LEAD_EMAIL,
        "to_email": inbox,
        "subject": TEST_SUBJECT,
        "date_iso": TEST_DATE_ISO,
        "internal_ms": 1723652400000,
    }

# Apply monkey patches
poll_inbox.poll_ids = _fake_poll_ids
classify_message.classify = _fake_classify


def _unit_checks():
    print("[scratch] Running unit checks for resolver and writers…")
    lead = find_lead_row(TEST_LEAD_EMAIL, TEST_INBOX)
    print(f"[scratch] resolver returned: {lead}")
    if not lead:
        print("[scratch] Resolver did not find the lead. Ensure the email exists in your CRM CSV.")
        return
    print("[scratch] Flipping Responded? to Yes via mark_yes…")
    ok_yes = mark_yes(TEST_LEAD_EMAIL, TEST_SUBJECT, TEST_DATE_ISO)
    print(f"[scratch] mark_yes returned: {ok_yes}")
    print("[scratch] Flipping Responded? to No via mark_no…")
    ok_no = mark_no(TEST_LEAD_EMAIL)
    print(f"[scratch] mark_no returned: {ok_no}")


def _one_tick_run():
    print("[scratch] Running one-tick runner with monkey patches…")
    counters = run_once_for_inbox(TEST_INBOX)
    print(f"[scratch] one-tick counters: {counters}")


if __name__ == "__main__":
    # 1) Quick unit checks (optional; comment out if you only want the one-tick)
    _unit_checks()

    # 2) One-tick end-to-end simulation without Gmail
    _one_tick_run()
