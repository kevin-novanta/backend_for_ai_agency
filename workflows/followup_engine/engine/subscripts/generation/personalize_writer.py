# engine/subscripts/generation/personalize_writer.py
from __future__ import annotations
from typing import Dict, Any

def _safe(s: Any) -> str:
    return (str(s) if s is not None else "").strip()

def personalize(generic: Dict[str, str], row: Dict[str, Any], fields_map: Dict[str, Any], *, followup_num: int) -> tuple[str, str]:
    """
    Take the generic draft and personalize lightly using CRM row fields.
    For now this is a deterministic stub (no API call); plug in your LLM later.
    Returns (subject, body).
    """
    subject = _safe(generic.get("subject"))
    body = _safe(generic.get("body"))

    # Light personalization hooks
    first = _safe(row.get("First Name"))
    company = _safe(row.get("Company Name"))
    opener_ref = _safe(row.get("Opener Subject Sent"))

    # Subject: add first name/company if useful
    if first and first.lower() not in subject.lower():
        subject = f"{first} — {subject}"
    elif company and company.lower() not in subject.lower():
        subject = f"{subject} · {company}"

    # Body: append a small personalized line referencing opener or company
    tail_bits = []
    if opener_ref:
        tail_bits.append(f"PS: Re your earlier note: “{opener_ref}”.")
    if company:
        tail_bits.append(f"(Saw a few updates at {company}—happy to tailor this.)")

    if tail_bits:
        body = f"{body}\n\n" + " ".join(tail_bits)

    return subject, body