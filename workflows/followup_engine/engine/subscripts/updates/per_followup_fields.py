from __future__ import annotations
from typing import Dict, Any, Optional
from datetime import datetime
from engine.subscripts.utils.crm_helpers import setf

__all__ = ["write_per_followup_fields"]

def _split_dt(dt_str: Optional[str]) -> tuple[str, str]:
    """Return (date_str, time_str) like ('2025-08-19', '13:45:00') from ISO-ish input."""
    if not dt_str:
        now = datetime.utcnow()
        return now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")
    s = dt_str.replace("T", " ").replace("Z", "")
    # naive split
    if " " in s:
        date_s, time_s = s.split(" ", 1)
    else:
        date_s, time_s = s, "00:00:00"
    return date_s.strip(), time_s.strip()

def write_per_followup_fields(
    row: Dict[str, Any],
    fields_map: Dict[str, Any],
    n: int,
    *,
    subject: str,
    body: str,
    send_dt: Optional[str] = None,
    bounce: Optional[str] = None,
) -> None:
    """Write Follow Up N Subject/Body/Time/Date/Bounce (N=1..6)."""
    pf = fields_map.get("per_followup_fields", {}).get(str(n), {})
    subj_col = pf.get("subject", f"Follow Up {n} Subject Sent")
    body_col = pf.get("body", f"Follow Up {n} Body Sent")
    time_col = pf.get("time", f"Follow Up {n} Time Sent")
    date_col = pf.get("date", f"Follow Up {n} Date Sent")
    bounce_col = pf.get("bounce", f"Bounce Status for Follow Up {n}")

    d_str, t_str = _split_dt(send_dt)

    setf(row, subj_col, subject or "")
    setf(row, body_col, body or "")
    setf(row, time_col, t_str)
    setf(row, date_col, d_str)
    if bounce is not None:
        setf(row, bounce_col, bounce)