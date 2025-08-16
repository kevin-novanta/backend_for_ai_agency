from __future__ import annotations
import os
import sys
import logging

from .runtime.runner import run_loop
from .Adapters.creds_loader import load_senders


def _load_inboxes() -> list[str]:
    """Load inboxes from env var INBOXES or fall back to Creds/email_accounts.json."""
    env = os.getenv("INBOXES", "").strip()
    if env:
        inboxes = [x.strip() for x in env.split(",") if x.strip()]
        if inboxes:
            print(f"[main] Using INBOXES from env: {inboxes}")
            return inboxes
    inboxes = load_senders()
    print(f"[main] Using inboxes from Creds/email_accounts.json: {inboxes}")
    return inboxes


def _get_int_env(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        print(f"[main] Invalid int for {name}={val!r}; falling back to {default}")
        return default


if __name__ == "__main__":
    # Basic logging (runner uses the shared logger)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    inboxes = _load_inboxes()
    if not inboxes:
        print("[main] No inboxes found. Set INBOXES env var or configure Creds/email_accounts.json")
        sys.exit(1)

    interval_sec = _get_int_env("INTERVAL_SEC", 60)
    jitter_sec = _get_int_env("JITTER_SEC", 15)
    lookback_minutes = _get_int_env("LOOKBACK_MINUTES", 1440)

    print(
        f"[main] Starting run_loop(inboxes={len(inboxes)}, interval={interval_sec}s, jitter={jitter_sec}s, lookback={lookback_minutes}m)"
    )

    try:
        run_loop(inboxes, interval_sec=interval_sec, jitter_sec=jitter_sec, lookback_minutes=lookback_minutes)
    except KeyboardInterrupt:
        print("\n[main] Stopped by user (Ctrl+C)")
        sys.exit(0)