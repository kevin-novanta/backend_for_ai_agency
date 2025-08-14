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