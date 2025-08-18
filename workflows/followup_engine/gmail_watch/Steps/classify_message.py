from __future__ import annotations
from typing import Dict, Optional
import email.utils
import datetime
import time
from googleapiclient.errors import HttpError

from ..Logic.filters import is_auto_reply
from ..Logic.mapping import extract_email

META_HEADERS = ["From", "To", "Subject", "Date", "In-Reply-To", "References"]


def _headers_to_dict(msg: Dict) -> Dict[str, str]:
    headers = msg.get("payload", {}).get("headers", [])
    return {h.get("name", ""): h.get("value", "") for h in headers}


def _parse_date_iso(date_hdr: str | None) -> str:
    if not date_hdr:
        return ""
    try:
        dt = email.utils.parsedate_to_datetime(date_hdr)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        # Normalize to UTC Z
        return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


def classify(svc, msg_id: str, inbox: str) -> Optional[dict]:
    """Fetch and classify a message via Gmail API. Return a normalized dict or None to skip."""
    user_id = "me"
    req = (
        svc.users()
        .messages()
        .get(userId=user_id, id=msg_id, format="metadata", metadataHeaders=META_HEADERS)
    )
    retries = 0
    while True:
        try:
            msg = req.execute()
            break
        except (HttpError, ConnectionResetError, TimeoutError) as e:
            if retries >= 3:
                raise
            sleep_s = [0.5, 1.0, 2.0][retries]
            print(f"[classify_message] transient error, retrying in {sleep_s}s for {msg_id}: {e}")
            time.sleep(sleep_s)
            retries += 1

    headers = msg.get("payload", {}).get("headers", [])
    print(f"[classify_message] Processing {msg_id}, headers={headers}")

    hdrs = _headers_to_dict(msg)
    subject = hdrs.get("Subject", "")

    # auto-reply / OOO filter based on subject+headers
    if is_auto_reply(subject, hdrs):
        return None

    raw_from = hdrs.get("From", "")
    raw_to = hdrs.get("To", "")

    from_email = (extract_email(raw_from) or "").lower()
    to_email = (extract_email(raw_to) or "").lower()

    date_iso = _parse_date_iso(hdrs.get("Date"))
    internal_ms = int(msg.get("internalDate", 0))

    # Threading signals
    thread_id = msg.get("threadId", "") or ""
    in_reply_to = hdrs.get("In-Reply-To", "") or ""

    parsed = {
        "from_email": from_email,
        "to_email": to_email,
        "subject": subject,
        "date_iso": date_iso,
        "internal_ms": internal_ms,
        "thread_id": thread_id,
        "in_reply_to": in_reply_to,
    }
    print(f"[classify_message] Parsed: {parsed}")
    return parsed