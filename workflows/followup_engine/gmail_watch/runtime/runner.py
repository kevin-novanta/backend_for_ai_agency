from __future__ import annotations
import time
import random
import traceback
from typing import Sequence, Dict

from ..Steps.poll_inbox import poll_ids
from ..Steps.classify_message import classify
from ..Steps.resolve_lead import find_lead_row, load_crm_index
from ..Steps.mark_responded import mark_yes
from ..Adapters.gmail_client import gmail_service_for_user
from ..State.offsets import get_offset, set_offset
from ..State.paths import logger


def _now_ms() -> int:
    return int(time.time() * 1000)


def run_once_for_inbox(inbox: str, lookback_minutes: int = 1440) -> Dict[str, int]:
    """Process one inbox for a single tick.

    Steps:
      1) Build Gmail service via DWD
      2) Determine since_ms watermark (fallback: last `lookback_minutes`)
      3) poll_ids -> list of message IDs
      4) For each ID: classify -> resolve lead -> mark responded
      5) Advance offset to the max internal_ms we observed
    """
    counts: Dict[str, int] = dict(checked=0, matched=0, updated=0, auto=0, skipped=0, errors=0)

    logger.info("[runner] >>> START inbox=%s", inbox)

    # Build Gmail API client
    try:
        svc = gmail_service_for_user(inbox)
        logger.info("[runner] gmail_service_for_user OK for %s", inbox)
    except Exception:
        counts["errors"] += 1
        logger.error("[runner] Failed to build Gmail service for %s: %s", inbox, traceback.format_exc())
        return counts

    # Determine since watermark
    try:
        since_ms = get_offset(inbox)
    except Exception:
        since_ms = None
        logger.error("[runner] get_offset failed for %s: %s", inbox, traceback.format_exc())

    if since_ms in (None, 0):
        since_ms = _now_ms() - lookback_minutes * 60 * 1000
        logger.info("[runner] using initial since_ms=%s (lookback_minutes=%s)", since_ms, lookback_minutes)
    else:
        logger.info("[runner] loaded since_ms=%s", since_ms)

    print(f"[runner] Running for {inbox} since={since_ms}")
    newest_ms = since_ms

    # Load CRM index once
    try:
        email_index = load_crm_index()
        lead_by_email = email_index.get("by_email", {})
        logger.info("[runner] CRM index loaded: %d emails", len(lead_by_email))
    except Exception:
        lead_by_email = {}
        logger.error("[runner] Failed to load CRM index: %s", traceback.format_exc())

    # Poll IDs
    try:
        ids = poll_ids(svc, inbox, since_ms, lookback_minutes)
        logger.info("[runner] poll_ids -> %d message(s) for %s", len(ids), inbox)
    except Exception:
        counts["errors"] += 1
        logger.error("[runner] poll_ids failed for %s: %s", inbox, traceback.format_exc())
        return counts

    for mid in ids:
        counts["checked"] += 1
        print(f"[runner] Checking msg_id={mid}")
        try:
            msg = classify(svc, mid, inbox)
            print(f"[runner] Classified: {msg}")
            logger.info("[runner] classify(%s) -> %s", mid, "OK" if msg else "None")
        except Exception:
            counts["errors"] += 1
            logger.error("[runner] classify failed for %s: %s", mid, traceback.format_exc())
            continue

        if not msg:
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: classify returned None for msg_id={mid}")
            continue

        # Drop if message is older than our watermark (in case search backfilled)
        try:
            im = int(msg.get("internal_ms") or 0)
        except Exception:
            im = 0
        if im and im <= since_ms:
            logger.info("[runner] skip old message id=%s internal_ms=%s <= since_ms=%s", mid, im, since_ms)
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: message internal_ms={im} <= since_ms={since_ms} for msg_id={mid}")
            continue

        # Self-sent filter and basic sanity
        from_email = (msg.get("from_email") or "").strip().lower()
        if not from_email:
            logger.info("[runner] skip: missing from_email for %s", mid)
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: missing from_email for msg_id={mid}")
            continue
        if from_email == inbox.strip().lower():
            logger.info("[runner] skip self-sent: %s", mid)
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: self-sent message for msg_id={mid}")
            continue

        # CSV-only filter: sender must exist in CRM index
        row = lead_by_email.get(from_email)
        if not row:
            logger.info("[runner] skip: from not in CRM from=%s (id=%s)", from_email, mid)
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: sender {from_email} not in CRM for msg_id={mid}")
            continue

        # Owner/inbox match if owner is present
        owner = (row.get("Owner / Assigned To") or "").strip().lower()
        if owner and owner != inbox.strip().lower():
            logger.info("[runner] skip: owner mismatch from=%s owner=%s inbox=%s", from_email, owner, inbox)
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: owner mismatch owner={owner} inbox={inbox} for msg_id={mid}")
            continue

        # Optional thread match if both provided
        stored_tid = (row.get("Email Thread Link") or "").strip()
        tid = (msg.get("thread_id") or "").strip()
        if stored_tid and tid and stored_tid != tid:
            logger.info("[runner] skip: thread mismatch from=%s tid=%s stored=%s", from_email, tid, stored_tid)
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: thread mismatch tid={tid} stored={stored_tid} for msg_id={mid}")
            continue

        # At this point, it's a reply from a known lead (and for this inbox if owner set)
        counts["matched"] += 1

        try:
            print(f"[runner] Marking lead {row['Email']} YES")
            ok = mark_yes(row.get("Email", from_email), msg.get("subject", ""), msg.get("date_iso", ""))
            if ok:
                counts["updated"] += 1
                logger.info("[runner] mark_yes OK for %s", from_email)
            else:
                logger.info("[runner] mark_yes returned False for %s", from_email)
        except Exception:
            counts["errors"] += 1
            logger.error("[runner] mark_yes failed for %s: %s", from_email, traceback.format_exc())

        # Track newest internal timestamp seen
        if im and im > newest_ms:
            newest_ms = im

    # Persist watermark
    try:
        if newest_ms and newest_ms > 0:
            set_offset(inbox, newest_ms)
            logger.info("[runner] set_offset(%s, %s)", inbox, newest_ms)
            print(f"[runner] Updated offset for {inbox} to {newest_ms}")
    except Exception:
        counts["errors"] += 1
        logger.error("[runner] set_offset failed for %s: %s", inbox, traceback.format_exc())

    logger.info("[runner] <<< END inbox=%s counts=%s", inbox, counts)
    return counts


def run_loop(inboxes: Sequence[str], interval_sec: int = 60, jitter_sec: int = 15, lookback_minutes: int = 1440):
    logger.info("Starting gmail_watch for %d inbox(es): %s", len(inboxes), ", ".join(inboxes))
    while True:
        for inbox in inboxes:
            try:
                c = run_once_for_inbox(inbox, lookback_minutes=lookback_minutes)
                logger.info(
                    "[loop] %s -> checked=%d matched=%d updated=%d auto=%d skipped=%d errors=%d",
                    inbox, c.get("checked",0), c.get("matched",0), c.get("updated",0), c.get("auto",0), c.get("skipped",0), c.get("errors",0)
                )
            except Exception:
                logger.error("[loop] Fatal error in inbox loop for %s: %s", inbox, traceback.format_exc())
        sleep_for = interval_sec + random.randint(0, max(0, jitter_sec))
        time.sleep(sleep_for)