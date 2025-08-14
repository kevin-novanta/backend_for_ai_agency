from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, Optional
from ..Logic.filters import is_auto_reply

def _headers_to_dict(msg: Dict) -> Dict[str, str]:
    headers = msg.get("payload", {}).get("headers", [])
    return {h.get("name", ""): h.get("value", "") for h in headers}

def classify(svc, msg_id: str, inbox: str) -> Optional[dict]:
    """Fetch and classify a message. Return a normalized dict or None to skip."""
    # TODO: use svc.users().messages().get(...).execute()
    msg = {"payload": {"headers": []}, "internalDate": 0}
    hdrs = _headers_to_dict(msg)
    subject = hdrs.get("Subject", "")
    if is_auto_reply(subject, hdrs):
        return None
    from_email = hdrs.get("From", "").lower()
    to_email = hdrs.get("To", "").lower()
    date_iso = datetime.now(timezone.utc).isoformat()
    internal_ms = int(msg.get("internalDate", 0))
    return {
        "from_email": from_email,
        "to_email": to_email,
        "subject": subject,
        "date_iso": date_iso,
        "internal_ms": internal_ms,
    }