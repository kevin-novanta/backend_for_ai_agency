from __future__ import annotations
from typing import Dict, Any
from engine.subscripts.utils.crm_helpers import setf

__all__ = ["write_last_sent_timestamps"]

def write_last_sent_timestamps(row: Dict[str, Any], fields_map: Dict[str, Any], iso_when: str) -> None:
    """
    Write both timestamp spellings:
      - Last Message Sent Time Stamp
      - Last Message Sent Timestamp
    """
    can = fields_map.get("canonical", {})
    a = can.get("last_sent_a", "Last Message Sent Time Stamp")
    b = can.get("last_sent_b", "Last Message Sent Timestamp")
    setf(row, a, iso_when)
    setf(row, b, iso_when)