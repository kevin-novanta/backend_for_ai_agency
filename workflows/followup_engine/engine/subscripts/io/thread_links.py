# Utilities to convert between Gmail thread IDs and Gmail UI links
# and to parse a thread ID back from a Gmail link.
#
# Supported forms:
#   https://mail.google.com/mail/u/0/#inbox/<threadId>
#   https://mail.google.com/mail/u/0/#all/<threadId>
#   https://mail.google.com/mail/u/0/#search/<threadId>
#   https://mail.google.com/mail/u/0/?th=<threadId>

from __future__ import annotations
import re
from urllib.parse import urlparse, parse_qs

__all__ = ["thread_id_to_link", "link_to_thread_id"]

# Matches anchors like /#inbox/<threadId>, /#all/<threadId>, /#search/<threadId>
_THREAD_ID_ANCHOR_RE = re.compile(r"/#(?:inbox|all|search)/([a-fA-F0-9]+)")


def thread_id_to_link(thread_id: str, account_index: int = 0) -> str:
    """Return a Gmail UI URL for a given thread id.

    Args:
        thread_id: The Gmail thread ID returned by the Gmail API (hex string).
        account_index: Gmail account index used in the web UI (u/0, u/1, ...).

    Returns:
        A Gmail web UI link that opens the thread in the inbox view.
    """
    thread_id = str(thread_id).strip()
    if not thread_id:
        raise ValueError("thread_id_to_link(): empty thread_id")
    return f"https://mail.google.com/mail/u/{account_index}/#inbox/{thread_id}"


def link_to_thread_id(link: str | None) -> str | None:
    """Extract a Gmail thread ID from a Gmail UI link.

    Supports common URL shapes, including hash anchors and the `?th=` query param.
    Returns None if the thread id cannot be parsed.
    """
    if not link:
        return None

    link = str(link).strip()
    if not link:
        return None

    # Try query param (?th=...)
    try:
        parsed = urlparse(link)
        qs = parse_qs(parsed.query)
        if "th" in qs and qs["th"]:
            return qs["th"][0]
    except Exception:
        pass

    # Try hash anchor forms (#inbox/<id>, #all/<id>, #search/<id>)
    m = _THREAD_ID_ANCHOR_RE.search(link)
    if m:
        return m.group(1)

    # Last path segment fallback if it looks like a hex id
    last = link.rstrip("/").split("/")[-1]
    if re.fullmatch(r"[a-fA-F0-9]+", last):
        return last

    return None