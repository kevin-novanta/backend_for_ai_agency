from __future__ import annotations
from typing import Optional, Dict, List, Tuple
import csv
import os

# ===== CRM CSV CONFIG =====
# Canonical path to your CRM CSV
_CSV_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/leads/CRM_Leads/CRM_leads_copy.csv"

# Candidate column names for the lead's email (case-insensitive)
_EMAIL_COLS = [
    "Email", "email", "Email Address", "E-mail", "Primary Email"
]

# Optional: columns to pass through if present (kept small; CSV can be huge)
_PASS_THROUGH_COLS = [
    "Email", "email", "Client Name", "Client", "Owner / Assigned To", "Owner", "Assigned To",
    "Responded?", "Last Inbound Timestamp", "Stop Reason", "Lead Stage", "Sequence Stage"
]

# ===== Helpers =====

def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()

def _find_email_key(fieldnames: List[str]) -> Optional[str]:
    if not fieldnames:
        return None
    lower_map = {fn.lower(): fn for fn in fieldnames}
    for cand in _EMAIL_COLS:
        k = lower_map.get(cand.lower())
        if k:
            return k
    return None

def _safe_open_csv(path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    if not os.path.exists(path):
        print(f"[resolve_lead] CSV not found at {_CSV_PATH}")
        return [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, (reader.fieldnames or [])

# ===== Public API =====

def find_lead_row(from_email: str, inbox: str) -> Optional[Dict[str, str]]:
    """Resolve a CRM row by prospect email from the canonical CSV.

    Args:
        from_email: Sender address from Gmail (prospect)
        inbox:      Our inbox address (unused here but kept for interface stability)

    Returns:
        A dict representing the matched lead row (slim subset of columns when possible),
        or None if no match is found.
    """
    norm_from = _normalize_email(from_email)
    if not norm_from:
        print("[resolve_lead] Empty from_email provided")
        return None

    rows, header = _safe_open_csv(_CSV_PATH)
    if not rows or not header:
        print("[resolve_lead] No rows or headers loaded from CSV")
        return None

    email_key = _find_email_key(header)
    if not email_key:
        print("[resolve_lead] Could not detect an email column in CSV headers")
        return None

    for r in rows:
        lead_email = _normalize_email(r.get(email_key))
        if lead_email and lead_email == norm_from:
            # Build a slimmed row preserving useful fields when present
            slim: Dict[str, str] = {}
            for k in _PASS_THROUGH_COLS:
                if k in r:
                    slim[k] = r.get(k, "")
            # Always include canonical 'Email'
            if "Email" not in slim:
                slim["Email"] = r.get(email_key, "")
            print(f"[resolve_lead] Match found for {norm_from} using column '{email_key}'")
            print(f"[resolve_lead] Match: {norm_from} -> {slim or r}")
            return slim or r

    print(f"[resolve_lead] No match for {norm_from}")
    print(f"[resolve_lead] No match for {norm_from}")
    return None


# (append below existing code)

def _load_rows_and_header() -> tuple[list[dict[str, str]], list[str]]:
    return _safe_open_csv(_CSV_PATH)


def load_crm_index() -> dict:
    """Load the CRM once and build fast lookup structures.
    Returns: {"by_email": Dict[email_lower -> row], "headers": List[str]}
    """
    rows, header = _load_rows_and_header()
    by_email: dict[str, dict[str, str]] = {}
    email_key = _find_email_key(header) if header else None
    if not email_key:
        print(f"[resolve_lead] Loaded {len(rows)} CRM rows")
        return {"by_email": by_email, "headers": header}

    for r in rows:
        e = _normalize_email(r.get(email_key))
        if e:
            by_email[e] = r
    print(f"[resolve_lead] Loaded {len(rows)} CRM rows")
    return {"by_email": by_email, "headers": header}