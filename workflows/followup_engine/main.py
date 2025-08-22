#!/usr/bin/env python3

from pathlib import Path
import sys, json, argparse
from datetime import datetime

# --- imports from your engine package ---
from engine.subscripts.io.load_crm import load_crm
from engine.subscripts.io.save_crm import save_row
from engine.subscripts.filters.by_client import filter_by_client
from engine.subscripts.filters.eligible_for_run import eligible_rows
from engine.subscripts.gating.responded_guard import is_replied
from engine.subscripts.gating.send_window import allowed_now
from engine.subscripts.gating.thread_guard import thread_guard
from engine.subscripts.selectors.next_touch import compute_next_followup_num
from engine.subscripts.selectors.owner_inbox import resolve_owner_inbox
from engine.subscripts.utils.crm_helpers import get, setf
from engine.subscripts.utils.dates import now_iso, delay_ok
from engine.subscripts.generation.build_context import build_context
from engine.subscripts.generation.generic_writer import draft_generic
from engine.subscripts.generation.personalize_writer import personalize
from engine.subscripts.sending.gmail_send import send_followup
from engine.subscripts.updates.messaging_status import set_status
from engine.subscripts.updates.stage_advance import advance_stage
from engine.subscripts.updates.timestamps import write_last_sent_timestamps
from engine.subscripts.updates.per_followup_fields import write_per_followup_fields
from engine.subscripts.updates.audit_log import log_action

ROOT = Path(__file__).resolve().parent
# Make sure the followup_engine directory is on sys.path so `engine` can be imported
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SETTINGS_DIR = ROOT / "engine" / "settings"

# Dry-run toggle (set before main runs)
DRY_RUN = False


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
            if not DRY_RUN:
                set_status(row, FIELDS, "Paused")
                save_row(csv_path, headers, row)
            log_action(client=client, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "replied", "dry_run": DRY_RUN})
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
        if not inbox:
            # No owner assigned → skip this lead per business rule
            log_action(client=client, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "no_owner_assigned"})
            continue

        # Require a thread link (existing or recovered); otherwise skip this lead
        ok_thread, info = thread_guard(
            row,
            FIELDS,
            inbox=inbox,
            dry_run=DRY_RUN,
            settings_dir=SETTINGS_DIR,
        )
        if not ok_thread:
            log_action(client=client, lead=lead_id, followup=None, inbox=inbox, result=info)
            print(f"[MAIN] Skipping {lead_id} — {info.get('reason', 'no_thread_link')}.")
            continue

        # Use the thread link for all downstream steps
        thread_link = info.get("thread_link")
        print(f"[MAIN] Using thread link for {lead_id}: {thread_link}")

        # 8) Build context and generate copy
        ctx = build_context(row, FIELDS, followup_num=next_n)
        generic = draft_generic(followup_num=next_n, context=ctx)
        subject, body = personalize(generic, row, FIELDS, followup_num=next_n)

        # 9) Mark Pending before send (skip in DRY_RUN)
        if not DRY_RUN:
            set_status(row, FIELDS, "Pending")
            save_row(csv_path, headers, row)

        # 10) Send (or simulate in DRY_RUN)
        if DRY_RUN:
            send_res = {"status": "ok", "sent_at": now_iso(), "dry_run": True, "thread_link": thread_link}
            print(f"[DRY RUN] Would send FU{next_n} to {lead_id} via {inbox} in thread {thread_link}")
        else:
            send_res = send_followup(
                inbox=inbox,
                to=lead_id,
                subject=subject,
                body=body,
                thread_link=thread_link,
            )
            # Persist thread link if newly created (normally shouldn't happen due to thread guard)
            new_thread = send_res.get("thread_link")
            if new_thread and not get(row, thread_col):
                setf(row, thread_col, new_thread)

        # 11) Persist CRM updates (only on real send success; skip in DRY_RUN)
        status = send_res.get("status", "ok")
        if status == "ok" and not DRY_RUN:
            print(f"[MAIN] Send succeeded for {lead_id}; updating CRM.")
            set_status(row, FIELDS, "Sent")
            advance_stage(row, FIELDS, next_n)
            # Dual timestamp columns
            sent_when = send_res.get("sent_at") or now_iso()
            write_last_sent_timestamps(row, FIELDS, sent_when)
            # Per-followup fields: subject/body/time/date/bounce
            write_per_followup_fields(
                row,
                FIELDS,
                next_n,
                subject=subject,
                body=body,
                send_dt=sent_when,
                bounce=send_res.get("bounce_status"),
            )
            save_row(csv_path, headers, row)
        else:
            print(f"[MAIN] Skipping CRM update for {lead_id} (dry run or send failed).")

        # 12) Audit
        log_action(client=client, lead=lead_id, followup=next_n, inbox=inbox, result=send_res)
        processed += 1

    print(f"Done. Processed {processed} lead(s) for '{client}'.")
    return 0


if __name__ == "__main__":
    # CLI flag + interactive prompt for DRY_RUN
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run in dry run mode (no sends, no CRM updates)")
    cli_args, unknown = parser.parse_known_args()
    if cli_args.dry_run:
        DRY_RUN = True
        print("[INIT] DRY RUN enabled via --dry-run. No emails will be sent, no CRM updates will be written.")
    else:
        try:
            choice = input("👉 Run in DRY RUN mode? (y/N): ").strip().lower()
        except EOFError:
            choice = ""
        if choice == "y":
            DRY_RUN = True
            print("[INIT] DRY RUN enabled. No emails will be sent, no CRM updates will be written.")
        else:
            print("[INIT] Live mode. Emails will be sent and CRM will be updated.")

    sys.exit(main())