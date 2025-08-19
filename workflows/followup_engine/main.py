

#!/usr/bin/env python3
"""
Follow-up Orchestrator (v1)

- Prompts for client
- Loads CRM (all clients), filters to client
- Skips leads with blank Sequence Stage
- Skips non-safe deliverability
- Honors reply guard + send window + delay rules
- Locks Owner / Assigned To to a single inbox
- Builds LLM context from opener + business fields
- Generates generic draft → personalizes → sends in same thread
- Updates Messaging Status, Sequence Stage, timestamps (both spellings)
- Writes per-followup columns (F1–F6 subject/body/time/date/bounce)
- Audits each action
"""

from pathlib import Path
import sys, json
from datetime import datetime

# --- imports from your subscripts ---
from subscripts.io.load_crm import load_crm
from subscripts.io.save_crm import save_row
from subscripts.filters.by_client import filter_by_client
from subscripts.filters.eligible_for_run import eligible_rows
from subscripts.gating.responded_guard import is_replied
from subscripts.gating.send_window import allowed_now
from subscripts.selectors.next_touch import compute_next_followup_num
from subscripts.selectors.owner_inbox import resolve_owner_inbox
from subscripts.utils.crm_helpers import get, setf
from subscripts.utils.dates import now_iso, delay_ok
from subscripts.generation.build_context import build_context
from subscripts.generation.generic_writer import draft_generic
from subscripts.generation.personalize_writer import personalize
from subscripts.sending.gmail_send import send_followup
from subscripts.updates.messaging_status import set_status
from subscripts.updates.stage_advance import advance_stage
from subscripts.updates.timestamps import write_last_sent_timestamps
from subscripts.updates.per_followup_fields import write_per_followup_fields
from subscripts.updates.audit_log import log_action

ROOT = Path(__file__).resolve().parent
SETTINGS_DIR = ROOT / "settings"


def _load_json(name: str):
    """Support either exact or case-variant names (e.g., Delays.json)."""
    for candidate in [name, name.lower(), name.capitalize()]:
        p = SETTINGS_DIR / candidate
        if p.exists():
            return json.loads(p.read_text())
    raise FileNotFoundError(f"Missing settings file: {name}")


FIELDS = _load_json("fields_map.json")
DELAYS = _load_json("Delays.json")
WINDOW = _load_json("send_window.json")


# Canonical field helpers — expect these keys to exist in settings/fields_map.json
CAN = FIELDS["canonical"]


def prompt_client() -> str:
    try:
        entered = input("Client to run follow-ups for: ").strip()
    except EOFError:
        entered = ""
    return entered


def _deliverability_safe(row: dict) -> bool:
    """Only send to Deliverability == 'Safe' (skip Risky/Catch All/blank)."""
    dv = (get(row, CAN.get("deliverability", "Deliverability")) or "").strip()
    return dv.lower() == "safe"


def main() -> int:
    client = prompt_client()
    if not client:
        print("No client entered. Exiting.")
        return 0

    # 1) Load CRM, filter to client, and require a non-empty Sequence Stage
    rows, headers, csv_path = load_crm()
    rows = filter_by_client(rows, FIELDS, client)
    rows = eligible_rows(rows, FIELDS)  # Sequence Stage must be non-empty

    if not rows:
        print(f"No eligible rows for client '{client}'.")
        return 0

    # 2) Gating: global send window
    ok, reason = allowed_now(WINDOW)
    if not ok:
        print(f"Blocked by send window: {reason}")
        return 0

    processed = 0
    for row in rows:
        lead_email_col = CAN["email"]
        seq_stage_col = CAN["sequence_stage"]
        thread_col = CAN["thread_link"]
        last_a_col = CAN["last_sent_a"]
        last_b_col = CAN["last_sent_b"]
        msg_status_col = CAN["messaging_status"]

        lead_id = get(row, lead_email_col)
        seq_stage = get(row, seq_stage_col)

        # 3) Skip if watcher marked as replied → pause this lead
        if is_replied(row, FIELDS):
            set_status(row, FIELDS, "Paused")
            save_row(csv_path, headers, row)
            log_action(client=client, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "replied"})
            continue

        # 4) Skip non-safe deliverability
        if not _deliverability_safe(row):
            log_action(client=client, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "deliverability_not_safe"})
            continue

        # 5) Decide next follow-up number from Sequence Stage
        next_n = compute_next_followup_num(seq_stage)  # e.g., 1..6
        if next_n is None:
            log_action(client=client, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "no_next_followup"})
            continue

        # 6) Honor delay rules (use either of the two timestamp spellings)
        last_sent = get(row, last_a_col) or get(row, last_b_col)
        if not delay_ok(DELAYS, seq_stage, last_sent):
            log_action(client=client, lead=lead_id, followup=next_n, inbox=None,
                       result={"status": "skip", "reason": "delay_not_met"})
            continue

        # 7) Resolve/lock the sender inbox from Owner / Assigned To
        inbox = resolve_owner_inbox(row, FIELDS)  # may write Owner / Assigned To if blank

        # 8) Build context and generate copy
        ctx = build_context(row, FIELDS, followup_num=next_n)
        generic = draft_generic(followup_num=next_n, context=ctx)
        subject, body = personalize(generic, row, FIELDS, followup_num=next_n)

        # 9) Mark Pending before send (so you can observe mid-run state if interrupted)
        set_status(row, FIELDS, "Pending")
        save_row(csv_path, headers, row)

        # 10) Send in same thread if we have a link; else start new and write it back
        thread_link = get(row, thread_col) or None
        send_res = send_followup(
            inbox=inbox,
            to=lead_id,
            subject=subject,
            body=body,
            thread_link=thread_link,
        )

        # Persist thread link if newly created
        new_thread = send_res.get("thread_link")
        if new_thread and not thread_link:
            setf(row, thread_col, new_thread)

        # 11) Persist CRM updates (Sent, advance stage, timestamps, followup fields)
        status = send_res.get("status", "ok")
        if status == "ok":
            set_status(row, FIELDS, "Sent")
            advance_stage(row, FIELDS, next_n)
            # Dual timestamp columns
            write_last_sent_timestamps(row, FIELDS, now_iso())
            # Per-followup fields: subject/body/time/date/bounce
            write_per_followup_fields(
                row,
                FIELDS,
                next_n,
                subject=subject,
                body=body,
                send_dt=send_res.get("sent_at"),
                bounce=send_res.get("bounce_status"),
            )
        else:
            # If skipped or error, keep Messaging Status as Pending to retry later;
            # optionally set to Paused on hard errors.
            pass

        save_row(csv_path, headers, row)

        # 12) Audit
        log_action(client=client, lead=lead_id, followup=next_n, inbox=inbox, result=send_res)
        processed += 1

    print(f"Done. Processed {processed} lead(s) for '{client}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())