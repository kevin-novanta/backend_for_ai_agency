

from __future__ import annotations
from typing import Any, Dict, Iterable, Optional

__all__ = [
    "get",
    "setf",
    "has_value",
    "ensure_str",
    "get_any",
]


def ensure_str(value: Any) -> str:
    """Return a safe string representation ("" for None)."""
    if value is None:
        return ""
    return str(value)


def has_value(value: Any) -> bool:
    """True if value is not None and not just whitespace when string."""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def get(row: Dict[str, Any], column: str, default: Any = None) -> Any:
    """Safe getter from a CRM row dict.

    Args:
        row: The CRM row (dict).
        column: Exact column header to read.
        default: Value to return if column is missing or None.
    """
    if row is None:
        return default
    val = row.get(column, default)
    # Normalize empty strings to default if default is not None
    if val is None:
        return default
    return val


def get_any(row: Dict[str, Any], columns: Iterable[str], default: Any = None) -> Any:
    """Return the first present value among multiple candidate column names."""
    if row is None:
        return default
    for col in columns:
        if col in row and row[col] is not None and (not isinstance(row[col], str) or row[col].strip() != ""):
            return row[col]
    return default


def setf(row: Dict[str, Any], column: str, value: Any) -> Dict[str, Any]:
    """Set a value on the CRM row dict (functional style: returns the row)."""
    if row is None:
        raise ValueError("row cannot be None")
    row[column] = value
    return row