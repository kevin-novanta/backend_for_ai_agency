

from __future__ import annotations
from typing import List, Dict, Any


def _norm(s: Any) -> str:
    result = (str(s) if s is not None else "").strip().casefold()
    print(f"_norm: input={s!r}, normalized={result!r}")
    return result


def filter_by_client(rows: List[Dict[str, Any]], fields_map: Dict[str, Any], client_name: str) -> List[Dict[str, Any]]:
    """Filter CRM rows by client name (case-insensitive, trims whitespace).

    Args:
        rows: List of CRM row dicts.
        fields_map: settings/fields_map.json content; uses canonical.client.
        client_name: Target client name to match.

    Returns:
        A list of rows whose Client Name exactly matches `client_name` (case-insensitive).
    """
    print(f"filter_by_client: received {len(rows)} rows initially")
    if not rows:
        return []

    can = fields_map.get("canonical", {})
    client_col = can.get("client", "Client Name")
    print(f"filter_by_client: resolved client column name: {client_col!r}")
    target = _norm(client_name)
    print(f"filter_by_client: normalized target client name: {target!r}")

    matched_rows = [r for r in rows if _norm(r.get(client_col)) == target]
    print(f"filter_by_client: number of rows matched: {len(matched_rows)}")
    return matched_rows


__all__ = ["filter_by_client"]