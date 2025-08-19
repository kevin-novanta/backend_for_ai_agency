# /Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/engine/subscripts/selectors/next_touch.py
from __future__ import annotations
import re
from typing import Optional

# Canonical ordered stages we recognize
_STAGE_ORDER = [
    "Opener Sent",
    "Follow Up 1 Sent",
    "Follow Up 2 Sent",
    "Follow Up 3 Sent",
    "Follow Up 4 Sent",
    "Follow Up 5 Sent",
    "Follow Up 6 Sent",
]

# Simple normalization helpers
def _norm(s: str) -> str:
    return (s or "").strip().lower()

def _is_opener_stage(stage: str) -> bool:
    s = _norm(stage)
    # handle "opener", "opener sent", "open", "open or sent"
    return (
        s == "opener sent"
        or s == "opener"
        or s == "open"
        or s == "open or sent"
        or s == "opener - sent"
    )

def _parse_followup_num(stage: str) -> Optional[int]:
    """
    Return the follow-up N (1..6) if stage mentions follow up N; else None.
    Accepts variations like 'Follow Up 2 Sent', 'follow-up 3', 'FU4', etc.
    """
    s = _norm(stage)

    # common patterns
    # e.g., "follow up 3 sent", "follow-up 3", "followup 3"
    m = re.search(r"follow[\s\-]?up\s*(\d)", s)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None

    # short form like "fu3"
    m2 = re.search(r"\bfu\s*(\d)\b", s)
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            return None

    return None

def compute_next_followup_num(sequence_stage: str) -> Optional[int]:
    """
    Given a current 'Sequence Stage' string, return the NEXT follow-up number to send (1..6),
    or None if no next follow-up should be sent.

    Rules:
    - Opener → return 1
    - Follow Up N → return N+1 (until 6)
    - Follow Up 6 (or higher) → None
    - Unknown/blank → None (eligible_rows should filter blanks already)
    """
    if not sequence_stage or not str(sequence_stage).strip():
        return None

    if _is_opener_stage(sequence_stage):
        return 1

    n = _parse_followup_num(sequence_stage)
    if n is None:
        # Try exact match to canonical list as a fallback
        try:
            idx = _STAGE_ORDER.index(sequence_stage.strip())
        except ValueError:
            return None
        # idx 0 = opener sent => next 1
        if idx == 0:
            return 1
        # idx 1..6 correspond to FU1..FU6
        n = idx  # because FU1 is index 1, etc.

    # cap at 6
    if n >= 6:
        return None
    return n + 1