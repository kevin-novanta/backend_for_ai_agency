from __future__ import annotations
from typing import List, Dict, Any, Tuple
import csv
from pathlib import Path

__all__ = ["save_row"]

def _read_csv(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        headers = rdr.fieldnames or []
        rows = [dict(r) for r in rdr]
    return headers, rows

def _write_csv(path: Path, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

def save_row(csv_path: Path, headers: List[str], row: Dict[str, Any], *, email_key: str = "Email") -> None:
    """
    Update the CSV row that matches by Email (exact match).
    Writes back only once per call (read → modify → write).
    """
    path = Path(csv_path)
    hdrs, rows = _read_csv(path)

    # Ensure we preserve any new columns used by the caller
    all_headers = list(dict.fromkeys(hdrs + headers))
    email_val = (row.get(email_key) or "").strip()

    updated = False
    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        if (r.get(email_key) or "").strip() == email_val and email_val:
            # Merge: prefer values from 'row' when present
            merged = dict(r)
            for h in all_headers:
                if h in row and row[h] is not None:
                    merged[h] = row[h]
            out_rows.append(merged)
            updated = True
        else:
            out_rows.append(r)

    # If not found, append (optional; comment out if you never want to append)
    if not updated and email_val:
        # ensure all headers exist
        for h in headers:
            if h not in all_headers:
                all_headers.append(h)
        out_rows.append(row)

    _write_csv(path, all_headers, out_rows)