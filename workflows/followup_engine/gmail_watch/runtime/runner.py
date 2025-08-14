from __future__ import annotations
import time, random, traceback
from typing import Sequence
from ..Steps.poll_inbox import poll_ids
from ..Steps.classify_message import classify
from ..Steps.resolve_lead import find_lead_row
from ..Steps.mark_responded import mark_yes
from ..Adapters.gmail_client import gmail_service_for_user
from ..State.offsets import get_offset, set_offset
from ..State.paths import logger

def run_once_for_inbox(inbox: str, lookback_minutes: int = 1440) -> dict:
    svc = gmail_service_for_user(inbox)   # build Gmail API client for that inbox
    since_ms = get_offset(inbox)          # watermark (0 if first run)
    ids = poll_ids(svc, inbox, since_ms, lookback_minutes)

    counts = dict(checked=0, matched=0, updated=0, auto=0, skipped=0, errors=0)
    newest_ms = since_ms
    for mid in ids:
        counts["checked"] += 1
        try:
            msg = classify(svc, mid, inbox)
            if not msg:
                counts["skipped"] += 1
                continue
            lead = find_lead_row(msg["from_email"], inbox)
            if not lead:
                counts["skipped"] += 1
                continue
            counts["matched"] += 1
            if mark_yes(lead["Email"], msg["subject"], msg["date_iso"]):
                counts["updated"] += 1
            newest_ms = max(newest_ms, msg["internal_ms"])
        except Exception:
            logger.error("Error processing %s: %s", mid, traceback.format_exc())
            counts["errors"] += 1
    if newest_ms > since_ms:
        set_offset(inbox, newest_ms)
    return counts

def run_loop(inboxes: Sequence[str], interval_sec: int = 60, jitter_sec: int = 15):
    logger.info("Starting gmail_watch for %d inbox(es): %s", len(inboxes), ", ".join(inboxes))
    while True:
        for inbox in inboxes:
            try:
                c = run_once_for_inbox(inbox)
                logger.info("[%s] checked=%d matched=%d updated=%d auto=%d skipped=%d errors=%d",
                            inbox, c["checked"], c["matched"], c["updated"], c["auto"], c["skipped"], c["errors"])
            except Exception:
                logger.exception("Fatal error in inbox loop for %s", inbox)
        sleep_for = interval_sec + random.randint(0, max(0, jitter_sec))
        time.sleep(sleep_for)