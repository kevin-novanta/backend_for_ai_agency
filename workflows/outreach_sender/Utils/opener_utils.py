import re
from typing import Tuple

def strip_bracketed(text: str) -> str:
    """
    Remove any content enclosed in [] (and the brackets themselves),
    even if there are multiple bracketed chunks. Then collapse extra spaces.
    """
    if not isinstance(text, str):
        return text

    patterns = [
        r"\[[^\]]*\]",   # [ ... ]
    ]

    cleaned = text
    changed = True
    # Repeat until no more bracketed segments remain (handles multiple occurrences)
    while changed:
        before = cleaned
        for pat in patterns:
            cleaned = re.sub(pat, "", cleaned)
        changed = (cleaned != before)

    # Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def remove_brackets(text: str) -> str:
    """
    Remove substrings enclosed in square brackets [ ... ] without altering spacing,
    line breaks, or other formatting outside of the removed bracket content.
    """
    if not isinstance(text, str):
        return text
    return re.sub(r"\[[^\]]*\]", "", text)

def sanitize_email_fields(subject: str, body: str) -> Tuple[str, str]:
    """
    Sanitize both subject and body by removing bracketed content without altering spacing.
    Returns (clean_subject, clean_body).
    """
    return remove_brackets(subject), remove_brackets(body)