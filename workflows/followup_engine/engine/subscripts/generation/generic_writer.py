# engine/subscripts/generation/generic_writer.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

PROMPTS_DIR = Path(__file__).resolve().parents[3] / "engine" / "prompts"

def _read_prompt(filename: str) -> str:
    p = PROMPTS_DIR / filename
    if p.exists():
        try:
            return p.read_text(encoding="utf-8")
        except Exception:
            pass
    # Fallback generic prompt text
    return (
        "You are drafting Follow-Up {followup_num} for an ongoing email thread. "
        "Reference the previous subject and body respectfully, keep it brief, and ask a simple question."
    )

def _safe(s: Any) -> str:
    return (str(s) if s is not None else "").strip()

def draft_generic(*, followup_num: int, context: Dict[str, Any]) -> Dict[str, str]:
    """
    Produce a base (generic) subject/body pair for follow-up N using a text prompt file.
    Returns a dict: {"subject": "...", "body": "..."} which personalize() can refine.
    """
    prompt_name = f"generic_followup_f{followup_num}.txt"
    tpl = _read_prompt(prompt_name)

    prev = context.get("previous_message", {}) or {}
    lead = context.get("lead", {}) or {}
    subject_hint = _safe(prev.get("subject"))
    body_hint = _safe(prev.get("body"))
    first = _safe(lead.get("first_name"))
    company = _safe(lead.get("company_name"))

    # very light templating for the stub; you can swap to LLM later
    draft_subject = f"Quick nudge — re: {subject_hint}" if subject_hint else f"Quick nudge ({company or 'following up'})"
    draft_body = tpl.format(
        followup_num=followup_num,
        previous_subject=subject_hint,
        previous_body=body_hint,
        first_name=first,
        company_name=company,
    )

    return {"subject": draft_subject, "body": draft_body}