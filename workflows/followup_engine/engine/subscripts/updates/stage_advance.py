from __future__ import annotations
from typing import Dict, Any
from engine.subscripts.utils.crm_helpers import setf

__all__ = ["advance_stage"]

def advance_stage(row: Dict[str, Any], fields_map: Dict[str, Any], next_n: int) -> None:
    """
    Advance Sequence Stage to 'Follow Up {next_n} Sent'.
    (We assume you're only running opener + follow-ups 1..6 here.)
    """
    can = fields_map.get("canonical", {})
    col = can.get("sequence_stage", "Sequence Stage")
    if not isinstance(next_n, int) or next_n < 1:
        return
    setf(row, col, f"Follow Up {next_n} Sent")