from __future__ import annotations
from typing import Dict, Any
from engine.subscripts.utils.crm_helpers import setf

__all__ = ["set_status"]

def set_status(row: Dict[str, Any], fields_map: Dict[str, Any], value: str) -> None:
    """Set Messaging Status to value (e.g., Pending, Sent, Paused)."""
    can = fields_map.get("canonical", {})
    col = can.get("messaging_status", "Messaging Status")
    setf(row, col, value)