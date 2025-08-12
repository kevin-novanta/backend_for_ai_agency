from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging

# --- logger setup (keeps .warn alias working) ---
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

def info(msg, *args, **kwargs):
    _logger.info(msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    _logger.error(msg, *args, **kwargs)

def warn(msg, *args, **kwargs):
    _logger.warning(msg, *args, **kwargs)

warning = warn

# --- path to your CRM CSV ---
CRM_CSV = Path("/Users/kevinnovanta/backend_for_ai_agency/data/leads/CRM_Leads/CRM_leads_copy.csv")

# ---- low-level helpers ----

def _load_rows() -> Tuple[List[Dict[str, str]], List[str]]:
    with open(CRM_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def _save_rows(rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    # Preserve header order if available; otherwise union from data
    if not fieldnames:
        # derive fieldnames from union of keys
        seen = set()
        for r in rows:
            seen.update(r.keys())
        fieldnames = list(seen)
    with open(CRM_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def _norm(s: Optional[str]) -> str:
    return " ".join((s or "").split()).lower()

# ---- public surface used by the runners/webhooks ----

def update_fields(lead_id: str, fields: Dict[str, str]) -> bool:
    """Update a row by lead_id (email is used as primary key fallback).
    Returns True if a row was updated.
    """
    rows, fieldnames = _load_rows()

    # heuristics to find row: match by Email first, else by DM Link/id columns
    key_candidates = ["Email", "id", "DM Link", "Lead ID"]

    def _matches(r: Dict[str, str]) -> bool:
        for k in key_candidates:
            if _norm(r.get(k)) == _norm(lead_id):
                return True
        return False

    updated = False
    for r in rows:
        if not _matches(r):
            continue
        for k, v in fields.items():
            r[k] = v
            if k not in fieldnames:
                fieldnames.append(k)
        updated = True
        # do not break: if duplicates exist, update them all consistently
    if updated:
        _save_rows(rows, fieldnames)
        return True
    else:
        warn(f"No CRM row found for lead_id '{lead_id}' when updating fields {fields}")
        return False


def set_responded(lead_id: str, yes: bool = True) -> bool:
    result = update_fields(lead_id, {"Responded?": "Yes" if yes else "No"})
    if not result:
        warn(f"No CRM row found for lead_id '{lead_id}' when setting responded={yes}")
    return result


def lookup_lead_id_by_email(email_addr: Optional[str]) -> Optional[str]:
    if not email_addr:
        return None
    rows, _ = _load_rows()
    for r in rows:
        if _norm(r.get("Email")) == _norm(email_addr):
            # We return the email as the canonical lead_id; you can switch to a UUID later
            return r.get("Email") or email_addr
    return None


def lookup_lead_id_by_thread(thread_key: Optional[str], from_email: Optional[str], to_email: Optional[str]) -> Optional[str]:
    """Resolve a lead by any of:
    - reply-to alias pattern like reply+<lead_id>@yourdomain
    - Message-Id/References key you stored (not persisted yet; you can expand later)
    - fallback to matching the contact's from_email against CRM Email
    """
    if thread_key:
        # Heuristic: reply+<encoded lead_id>@domain
        import re
        m = re.search(r"reply\+([^@]+)@", thread_key)
        if m:
            return m.group(1)
    # Fallback: try the sender's email in CRM
    lid = lookup_lead_id_by_email(from_email)
    if lid:
        return lid
    # As a last resort, try the to_email if you store prospect email there
    return lookup_lead_id_by_email(to_email)


def is_automatic_reply(subject: Optional[str], body: Optional[str]) -> bool:
    s = (subject or "").lower()
    b = (body or "").lower()
    markers = (
        "out of office",
        "auto-reply",
        "autoreply",
        "automatic reply",
        "vacation responder",
        "delivery status notification",
        "delivery failure",
        "undeliverable",
        "mailer-daemon",
    )
    if any(m in s for m in markers):
        return True
    # common body signals
    if "i am currently out" in b or "i'm currently out" in b:
        return True
    return False

# --- convenience read helper for smoke tests / prompts ---

def get_fields(lead_id: str, field_list: list[str]) -> dict:
    rows, _ = _load_rows()
    out: dict = {}
    # match by Email first, then id/DM Link/Lead ID
    candidates = ("Email", "id", "DM Link", "Lead ID")

    def _matches(r):
        return any(_norm(r.get(k)) == _norm(lead_id) for k in candidates)

    for r in rows:
        if _matches(r):
            for f in field_list:
                out[f] = r.get(f, "")
            break
    return out