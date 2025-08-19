# /Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/engine/filters/eligible_for_run.py

from __future__ import annotations
from typing import List, Dict, Any

def _has_value(v: Any) -> bool:
    if v is None:
        print(f"_has_value: input={v!r}, has_value=False")
        return False
    if isinstance(v, str):
        result = v.strip() != ""
        print(f"_has_value: input={v!r}, has_value={result}")
        return result
    print(f"_has_value: input={v!r}, has_value=True")
    return True

def eligible_rows(rows: List[Dict[str, Any]], fields_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Keep rows where Sequence Stage is non-empty.
    (Deliverability, reply status, and time gating are handled elsewhere.)
    """
    print(f"eligible_rows: received {len(rows)} rows initially")
    if not rows:
        return []
    can = fields_map.get("canonical", {})
    seq_col = can.get("sequence_stage", "Sequence Stage")
    print(f"eligible_rows: resolved sequence stage column name: {seq_col!r}")
    matched_rows = [r for r in rows if _has_value(r.get(seq_col))]
    print(f"eligible_rows: number of rows matched: {len(matched_rows)}")
    return matched_rows

__all__ = ["eligible_rows"]