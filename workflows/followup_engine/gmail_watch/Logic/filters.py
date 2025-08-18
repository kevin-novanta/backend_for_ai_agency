from __future__ import annotations
import re
from typing import Dict

_AUTO_SUBJECT = re.compile(r"(out of office|auto.?reply|undeliverable|delivery status|vacation)", re.I)

def is_auto_reply(subject: str | None, headers: Dict[str, str]) -> bool:
    subj = subject or ""
    if _AUTO_SUBJECT.search(subj):
        return True
    auto = headers.get("Auto-Submitted", "").lower()
    if auto in ("auto-replied", "auto-generated"):
        return True
    if "List-Id" in headers or headers.get("Precedence", "").lower() in ("bulk", "list", "junk"):
        return True
    return False

# --- Bulk/no-reply sender helper ---
_NOREPLY_RE = re.compile(r"(no[-_]?reply|notification|noreply)@", re.I)
_BULK_DOMAINS = (
    "linkedin.com", "facebookmail.com", "twitter.com", "mailchimp.com",
    "sendgrid.net", "amazonses.com", "hubspotemail.net", "marketo.net"
)

def is_bulk_sender_domain(addr: str | None) -> bool:
    if not addr:
        return False
    a = addr.lower()
    if _NOREPLY_RE.search(a):
        return True
    for dom in _BULK_DOMAINS:
        if a.endswith("@" + dom) or a.endswith("." + dom) or a.endswith(dom):
            return True
    return False