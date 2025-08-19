# engine/subscripts/generation/build_context.py
from __future__ import annotations
from typing import Dict, Any

def _get(fields_map: Dict[str, Any], *keys: str, default: Any = "") -> Any:
    """Safely walk nested dict keys."""
    cur = fields_map
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _prior_fields_for_followup(fields_map: Dict[str, Any], followup_num: int) -> Dict[str, str]:
    """
    Return the column names for the message we should reference:
      - If F1, reference opener subject/body
      - If F2..F6, reference Follow Up (N-1) subject/body
    """
    per = fields_map.get("per_followup_fields", {})
    if followup_num <= 1:
        # opener
        return {
            "subject_col": _get(fields_map, "per_followup_fields", "opener", "subject", default="Opener Subject Sent"),
            "body_col": _get(fields_map, "per_followup_fields", "opener", "body", default="Opener Body Sent"),
        }
    prev_key = str(followup_num - 1)
    prev_map = per.get(prev_key, {})
    return {
        "subject_col": prev_map.get("subject", f"Follow Up {followup_num - 1} Subject Sent"),
        "body_col": prev_map.get("body", f"Follow Up {followup_num - 1} Body Sent"),
    }

def build_context(row: Dict[str, Any], fields_map: Dict[str, Any], *, followup_num: int) -> Dict[str, Any]:
    """
    Build an LLM-friendly context object for generating a follow-up.
    - Includes previous subject/body (opener or prior follow-up)
    - Includes lead identity and business fields for personalization
    - Mirrors your CRM column names so writers can reference them
    """
    can = fields_map.get("canonical", {})
    seq_col = can.get("sequence_stage", "Sequence Stage")
    email_col = can.get("email", "Email")

    # Where to read the prior message from (opener or FU N-1)
    pf = _prior_fields_for_followup(fields_map, followup_num)
    prev_subject = (row.get(pf["subject_col"]) or "").strip()
    prev_body = (row.get(pf["body_col"]) or "").strip()

    # Common lead/business fields you listed in your schema
    lead = {
        "email": (row.get(email_col) or "").strip(),
        "first_name": (row.get("First Name") or "").strip(),
        "last_name": (row.get("Last Name") or "").strip(),
        "company_name": (row.get("Company Name") or "").strip(),
        "phone_number": (row.get("Phone Number") or "").strip(),
        "address": (row.get("Address") or "").strip(),
        "custom_1": (row.get("Custom 1") or "").strip(),
        "custom_2": (row.get("Custom 2") or "").strip(),
        "custom_3": (row.get("Custom 3") or "").strip(),
        "campaign_type": (row.get("Campaign Type") or "").strip(),
        "deliverability": (row.get("Deliverability") or "").strip(),
    }

    context = {
        "followup_num": followup_num,
        "sequence_stage": (row.get(seq_col) or "").strip(),
        "previous_message": {
            "subject": prev_subject,
            "body": prev_body,
            "source_columns": pf,  # which columns we read from
        },
        "lead": lead,
        "raw_row": row,  # optional: full row if your writers want more fields
    }
    return context