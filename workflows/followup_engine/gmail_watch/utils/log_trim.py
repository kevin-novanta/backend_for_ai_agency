

#!/usr/bin/env python3
"""
Lightweight log file trimmer.

Keeps the last `keep_last` lines whenever a log grows past `max_lines`.
Safe to call repeatedly; it will do nothing if thresholds aren't exceeded.

Usages:
  - As a module:
        from workflows.followup_engine.gmail_watch.utils.log_trim import trim_log
        trim_log()

  - CLI (defaults baked in):
        python3 -m workflows.followup_engine.gmail_watch.utils.log_trim

  - CLI with overrides:
        python3 -m workflows.followup_engine.gmail_watch.utils.log_trim \
            --log /path/to/log.log --max-lines 10000 --keep-last 5000

Environment overrides:
  - GMAIL_WATCH_LOG_PATH
  - GMAIL_WATCH_LOG_MAX_LINES
  - GMAIL_WATCH_LOG_KEEP_LAST
"""
from __future__ import annotations

import argparse
import os
from collections import deque
from typing import Optional

# Defaults align with the runner integration you’re using
DEFAULT_LOG_PATH = (
    os.getenv(
        "GMAIL_WATCH_LOG_PATH",
        "/Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/gmail_watch/utils/gmail_watcher.log",
    )
)
DEFAULT_MAX_LINES = int(os.getenv("GMAIL_WATCH_LOG_MAX_LINES", "10000"))
DEFAULT_KEEP_LAST = int(os.getenv("GMAIL_WATCH_LOG_KEEP_LAST", "5000"))


def trim_log(
    log_path: Optional[str] = None,
    *,
    max_lines: int = DEFAULT_MAX_LINES,
    keep_last: int = DEFAULT_KEEP_LAST,
    encoding: str = "utf-8",
) -> tuple[bool, int, int]:
    """Trim a log file in-place if it exceeds `max_lines`.

    Returns (trimmed, total_lines_before, kept_lines_after).
    If the file doesn't exist, returns (False, 0, 0).
    Never raises for common IO errors; safe for cron.
    """
    path = log_path or DEFAULT_LOG_PATH
    try:
        if not path or not os.path.isfile(path):
            return (False, 0, 0)

        total = 0
        tail = deque(maxlen=keep_last)
        with open(path, "r", encoding=encoding, errors="ignore") as f:
            for line in f:
                total += 1
                tail.append(line)

        if total > max_lines:
            tmp = path + ".tmp"
            # Ensure folder exists to avoid edge cases where path is moved
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(tmp, "w", encoding=encoding) as out:
                out.writelines(tail)
            os.replace(tmp, path)
            return (True, total, len(tail))
        else:
            return (False, total, total)
    except Exception:
        # Fail quiet for robustness in scheduled runs
        return (False, 0, 0)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Trim a log file to last N lines.")
    p.add_argument(
        "--log",
        dest="log_path",
        default=DEFAULT_LOG_PATH,
        help=f"Path to log (default: %(default)s or env GMAIL_WATCH_LOG_PATH)",
    )
    p.add_argument(
        "--max-lines",
        type=int,
        default=DEFAULT_MAX_LINES,
        help="Threshold above which trimming happens (default: %(default)s)",
    )
    p.add_argument(
        "--keep-last",
        type=int,
        default=DEFAULT_KEEP_LAST,
        help="How many lines to keep after trimming (default: %(default)s)",
    )
    p.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Quiet mode (no output on success)",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    trimmed, total_before, total_after = trim_log(
        args.log_path, max_lines=args.max_lines, keep_last=args.keep_last
    )

    if args.quiet:
        return 0

    if total_before == 0 and not trimmed:
        print(f"No action: log not found or unreadable: {args.log_path}")
        return 0

    if trimmed:
        print(
            f"Trimmed '{args.log_path}': was {total_before} lines -> kept last {total_after}"
        )
    else:
        print(
            f"No trim needed for '{args.log_path}': {total_before} lines (<= {args.max_lines})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())