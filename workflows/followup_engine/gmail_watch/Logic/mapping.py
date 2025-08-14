from __future__ import annotations
import re
from typing import Optional

_EMAIL_RE = re.compile(r"<([^>]+)>")

def extract_email(addr: str | None) -> Optional[str]:
    if not addr:
        return None
    m = _EMAIL_RE.search(addr)
    email = (m.group(1) if m else addr).strip().lower()
    return email or None