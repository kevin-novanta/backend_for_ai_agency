from __future__ import annotations
from typing import List, Dict, Tuple
from pathlib import Path

# Reuse your existing helpers
from subscripts.selectors.next_touch import compute_next_followup_num
from subscripts.selectors.owner_inbox import resolve_owner_inbox
from subscripts.gating.responded_guard import is_replied
from subscripts.gating.send_window import allowed_now
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


def run(rows: List[Dict], headers: List[str], csv_path: Path, *, fields_map: Dict, delays_cfg: Dict, window_cfg: Dict) -> int:
    """Lightweight sequence runner for *opener follow-ups*.
    Mirrors the main orchestrator loop so you can call it independently if needed.

    Returns number of processed leads.
    """
    if not rows:
        return 0

    ok, reason = allowed_now(window_cfg)
    if not ok:
        print(f"[opener_followups] send window blocked: {reason}")
        return 0

    CAN = fields_map["canonical"]
    processed = 0

    for row in rows:
        lead_email_col = CAN["email"]
        seq_stage_col = CAN["sequence_stage"]
        thread_col = CAN["thread_link"]
        last_a_col = CAN["last_sent_a"]
        last_b_col = CAN["last_sent_b"]

        lead_id = get(row, lead_email_col)
        seq_stage = get(row, seq_stage_col)

        # guard: replied
        if is_replied(row, fields_map):
            set_status(row, fields_map, "Paused")
            log_action(client=None, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "replied"})
            continue

        # next follow-up number
        next_n = compute_next_followup_num(seq_stage)
        if next_n is None:
            log_action(client=None, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "no_next_followup"})
            continue

        # cadence delay
        last_sent = get(row, last_a_col) or get(row, last_b_col)
        if not delay_ok(delays_cfg, seq_stage, last_sent):
            log_action(client=None, lead=lead_id, followup=next_n, inbox=None,
                       result={"status": "skip", "reason": "delay_not_met"})
            continue

        # resolve owner
        inbox = resolve_owner_inbox(row, fields_map)
        if not inbox:
            log_action(client=None, lead=lead_id, followup=None, inbox=None,
                       result={"status": "skip", "reason": "no_owner_assigned"})
            continue

        # build + write copy
        ctx = build_context(row, fields_map, followup_num=next_n)
        generic = draft_generic(followup_num=next_n, context=ctx)
        subject, body = personalize(generic, row, fields_map, followup_num=next_n)

        set_status(row, fields_map, "Pending")

        # send in thread if present
        thread_link = get(row, thread_col) or None
        send_res = send_followup(inbox=inbox, to=lead_id, subject=subject, body=body, thread_link=thread_link)

        new_thread = send_res.get("thread_link")
        if new_thread and not thread_link:
            setf(row, thread_col, new_thread)

        if send_res.get("status", "ok") == "ok":
            set_status(row, fields_map, "Sent")
            advance_stage(row, fields_map, next_n)
            write_last_sent_timestamps(row, fields_map, now_iso())
            write_per_followup_fields(
                row,
                fields_map,
                next_n,
                subject=subject,
                body=body,
                send_dt=send_res.get("sent_at"),
                bounce=send_res.get("bounce_status"),
            )
        log_action(client=None, lead=lead_id, followup=next_n, inbox=inbox, result=send_res)
        processed += 1

    return processed
