from __future__ import annotations
import time
import random
import traceback
from typing import Sequence, Dict


import sys
import os

# Redirect stdout and stderr to log file
LOG_PATH = os.path.expanduser("/Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/gmail_watch/utils/gmail_watcher.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
sys.stdout = open(LOG_PATH, "a", buffering=1, encoding="utf-8")
sys.stderr = open(LOG_PATH, "a", buffering=1, encoding="utf-8")

# ---- Safe trim_log import & helper ----
try:
    # package-relative preferred
    from ..utils.trim_log import trim_log  # type: ignore
except Exception:
    try:
        # absolute fallback (when run as a module from repo root)
        from workflows.followup_engine.gmail_watch.utils.log_trim import trim_log  # type: ignore
    except Exception:
        from collections import deque
        def trim_log(*, log_path: str, max_lines: int = 10000, keep_last: int = 5000):  # type: ignore
            try:
                # Fast size guard
                try:
                    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                        # Count up to max_lines + 1 quickly and bail early if small
                        for i, _ in enumerate(f, 1):
                            if i > max_lines:
                                break
                        else:
                            # File ended before exceeding max_lines
                            return
                except FileNotFoundError:
                    return

                # Keep only the last `keep_last` lines using a deque
                dq = deque(maxlen=max(1, keep_last))
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        dq.append(line)

                tmp_path = f"{log_path}.tmp"
                with open(tmp_path, "w", encoding="utf-8") as out:
                    out.writelines(dq)

                os.replace(tmp_path, log_path)
                print(f"[runner] INFO: trimmed log to last {len(dq)} lines at {log_path}")
            except Exception as _e:
                print(f"[runner] ERROR: trim_log fallback failed: {_e}")


def _trim_log_safely():
    """Best-effort log trimming with verbose prints for debugging."""
    try:
        print(f"[runner] DEBUG: invoking trim_log on {LOG_PATH}")
        trim_log(log_path=LOG_PATH, max_lines=10000, keep_last=5000)
        print("[runner] DEBUG: trim_log completed")
    except Exception:
        print("[runner] ERROR: trim_log raised an exception; continuing")

from ..Steps.poll_inbox import poll_ids
from ..Steps.classify_message import classify
from ..Steps.resolve_lead import find_lead_row, load_crm_index
from ..Steps.mark_responded import mark_yes
from ..Adapters.gmail_client import gmail_service_for_user
from ..State.offsets import get_offset, set_offset
from ..State.paths import logger

# --- Config toggles and helpers ---
# --- Config toggles and helpers ---
# --- Config toggles and helpers ---
import json

STRICT_OWNER = os.getenv("GMAIL_WATCH_STRICT_OWNER", "1") not in ("0","false","False")
ENFORCE_THREAD_MATCH = os.getenv("GMAIL_WATCH_ENFORCE_THREAD", "0") in ("1","true","True")
AUDIT_LOG_PATH = os.getenv("GMAIL_WATCH_AUDIT_LOG", "/Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/gmail_watch/Data/reply_events.log.jsonl")
POLL_MINUTES = int(os.getenv("GMAIL_WATCH_POLL_MINUTES", "5"))

def _audit_event(payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(AUDIT_LOG_PATH), exist_ok=True)
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # best-effort; don't crash runner
        logger.warning("[runner] audit write failed", exc_info=True)

# Bulk/no-reply sender helper
from ..Logic.filters import is_bulk_sender_domain


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

        # Drop obvious bulk/no-reply domains early (defense-in-depth)
        if is_bulk_sender_domain(from_email):
            logger.info("[runner] skip: bulk/no-reply sender %s (id=%s)", from_email, mid)
            counts["skipped"] += 1
            print(f"[runner] Skipping reason: bulk/no-reply sender {from_email} for msg_id={mid}")
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
            if STRICT_OWNER:
                logger.info("[runner] skip: owner mismatch from=%s owner=%s inbox=%s", from_email, owner, inbox)
                counts["skipped"] += 1
                print(f"[runner] Skipping reason: owner mismatch owner={owner} inbox={inbox} for msg_id={mid}")
                continue
            else:
                logger.warning("[runner] owner mismatch (soft) from=%s owner=%s inbox=%s", from_email, owner, inbox)

        # Optional thread match if both provided
        stored_tid = (row.get("Email Thread Link") or "").strip()
        tid = (msg.get("thread_id") or "").strip()
        if stored_tid and tid and stored_tid != tid:
            if ENFORCE_THREAD_MATCH:
                logger.info("[runner] skip: thread mismatch from=%s tid=%s stored=%s", from_email, tid, stored_tid)
                counts["skipped"] += 1
                print(f"[runner] Skipping reason: thread mismatch tid={tid} stored={stored_tid} for msg_id={mid}")
                continue
            else:
                logger.warning("[runner] thread mismatch (soft) from=%s tid=%s stored=%s", from_email, tid, stored_tid)

        # At this point, it's a reply from a known lead (and for this inbox if owner set)
        counts["matched"] += 1

        try:
            print(f"[runner] Marking lead {row['Email']} YES")
            ok = mark_yes(
                row.get("Email", from_email),
                msg.get("subject", ""),
                msg.get("date_iso", ""),
                msg.get("thread_id", "")
            )
            if ok:
                counts["updated"] += 1
                logger.info("[runner] mark_yes OK for %s", from_email)
                _audit_event({
                    "inbox": inbox,
                    "lead_email": row.get("Email", from_email),
                    "from_email": from_email,
                    "subject": msg.get("subject",""),
                    "date_iso": msg.get("date_iso",""),
                    "thread_id": msg.get("thread_id",""),
                    "reason": "UPDATED"
                })
            else:
                logger.info("[runner] mark_yes returned False for %s", from_email)
                _audit_event({
                    "inbox": inbox,
                    "lead_email": row.get("Email", from_email),
                    "from_email": from_email,
                    "subject": msg.get("subject",""),
                    "date_iso": msg.get("date_iso",""),
                    "thread_id": msg.get("thread_id",""),
                    "reason": "NOUPDATE"
                })
        except Exception:
            counts["errors"] += 1
            logger.error("[runner] mark_yes failed for %s: %s", from_email, traceback.format_exc())

        # Track newest internal timestamp seen
        if im and im > newest_ms:
            newest_ms = im

    # Persist watermark
    try:
        if newest_ms and newest_ms > 0:
            bumped = int(newest_ms) + 1  # avoid equality tie on next tick
            set_offset(inbox, bumped)
            logger.info("[runner] set_offset(%s, %s)", inbox, bumped)
            print(f"[runner] Updated offset for {inbox} to {bumped}")
    except Exception:
        counts["errors"] += 1
        logger.error("[runner] set_offset failed for %s: %s", inbox, traceback.format_exc())

    logger.info("[runner] <<< END inbox=%s counts=%s", inbox, counts)
    return counts


def run_loop(inboxes: Sequence[str], interval_sec: int | None = None, jitter_sec: int = 15, lookback_minutes: int | None = None):
    if interval_sec is None:
        interval_sec = max(1, POLL_MINUTES) * 60
    if lookback_minutes is None:
        lookback_minutes = max(1, POLL_MINUTES)
    logger.info("[runner] Using poll interval=%sm, lookback window=%sm", interval_sec // 60, lookback_minutes)
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
        # After processing all inboxes this cycle, ensure log trimming runs once per cycle
        print("[runner] DEBUG: cycle complete; checking log size for trimming…")
        _trim_log_safely()
        sleep_for = interval_sec + random.randint(0, max(0, jitter_sec))
        time.sleep(sleep_for)

if __name__ == "__main__":
    # Lightweight CLI so you can run this module directly:
    #   python3 -m workflows.followup_engine.gmail_watch.runtime.runner --mode tick
    #   python3 -m workflows.followup_engine.gmail_watch.runtime.runner --mode loop --interval 60 --jitter 15
    import argparse
    import logging

    from ..Adapters.creds_loader import load_senders  # lazy import to avoid circulars

    parser = argparse.ArgumentParser(description="gmail_watch runner")
    parser.add_argument(
        "--mode",
        choices=["tick", "loop"],
        default="tick",
        help="tick = run one pass; loop = run forever with sleep",
    )
    parser.add_argument(
        "--inbox",
        action="append",
        default=None,
        help="Email address to process. Can be specified multiple times. Defaults to all from Creds/email_accounts.json",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=None,
        help="Lookback window (minutes) used when no stored offset exists. Defaults to --poll-minutes when unset.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Loop sleep interval seconds (only for --mode loop). Defaults to --poll-minutes * 60.",
    )
    parser.add_argument(
        "--poll-minutes",
        type=int,
        default=int(os.getenv("GMAIL_WATCH_POLL_MINUTES", "5")),
        help="Polling cadence in minutes. Also used as default lookback window when no offset exists.",
    )
    parser.add_argument(
        "--jitter",
        type=int,
        default=15,
        help="Random jitter seconds added to sleep (only for --mode loop).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    parser.add_argument(
        "--strict-owner",
        dest="strict_owner",
        action="store_true",
        help="Require Owner / Assigned To to equal inbox (default if env GMAIL_WATCH_STRICT_OWNER not set to 0/false).",
    )
    parser.add_argument(
        "--no-strict-owner",
        dest="strict_owner",
        action="store_false",
        help="Do not strictly enforce owner/inbox match (soft warning).",
    )
    parser.set_defaults(strict_owner=(os.getenv("GMAIL_WATCH_STRICT_OWNER", "1").lower() not in ("0", "false")))
    parser.add_argument(
        "--enforce-thread",
        dest="enforce_thread",
        action="store_true",
        help="Require Gmail thread_id to match CSV 'Email Thread Link' exactly when present.",
    )
    parser.add_argument(
        "--no-enforce-thread",
        dest="enforce_thread",
        action="store_false",
        help="Do not strictly enforce thread match (soft warning).",
    )
    parser.set_defaults(enforce_thread=(os.getenv("GMAIL_WATCH_ENFORCE_THREAD", "0").lower() in ("1", "true")))

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")

    # Derive interval/lookback defaults from poll-minutes if not explicitly provided
    if args.interval is None:
        args.interval = max(1, args.poll_minutes) * 60
    if args.lookback_minutes is None:
        args.lookback_minutes = max(1, args.poll_minutes)

    # Keep env in sync for any downstream reads
    os.environ["GMAIL_WATCH_POLL_MINUTES"] = str(args.poll_minutes)

    # Push CLI overrides into environment so the module-level toggles pick them up
    os.environ["GMAIL_WATCH_STRICT_OWNER"] = "1" if args.strict_owner else "0"
    os.environ["GMAIL_WATCH_ENFORCE_THREAD"] = "1" if args.enforce_thread else "0"

    # Re-evaluate toggles for this process (no global needed at module scope)
    STRICT_OWNER = os.getenv("GMAIL_WATCH_STRICT_OWNER", "1") not in ("0", "false", "False")
    ENFORCE_THREAD_MATCH = os.getenv("GMAIL_WATCH_ENFORCE_THREAD", "0") in ("1", "true", "True")

    # Resolve inbox list
    inboxes = args.inbox if args.inbox else load_senders()
    if not inboxes:
        raise SystemExit("No inboxes configured. Check Creds/email_accounts.json or pass --inbox.")

    print(f"[runner __main__] Mode={args.mode} Inboxes={inboxes} "
          f"Poll={args.poll_minutes}m Lookback={args.lookback_minutes}m StrictOwner={STRICT_OWNER} EnforceThread={ENFORCE_THREAD_MATCH}")


    if args.mode == "tick":
        results = {}
        for ib in inboxes:
            print(f"\n=== ONE-TICK for {ib} ===")
            results[ib] = run_once_for_inbox(ib, lookback_minutes=args.lookback_minutes)
            print(f"[runner __main__] {ib} -> {results[ib]}")
            print(f"[runner __main__] DEBUG: Checking log size before trim at: {LOG_PATH}")
            _trim_log_safely()
    else:
        # loop mode
        run_loop(inboxes, interval_sec=args.interval, jitter_sec=args.jitter, lookback_minutes=args.lookback_minutes)