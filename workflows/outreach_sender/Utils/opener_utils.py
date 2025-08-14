
import re
from typing import Tuple

# === SPAM TRIGGERS FOR CONSERVATIVE SCRUBBING ===
SPAM_WORDS = [
    "free", "discount", "guaranteed", "guarantee", "bonus", "sale", "special offer",
    "no obligation", "risk-free", "click here", "instant", "act now", "urgent",
    "limited time", "last chance", "hurry", "exclusive offer", "priority",
    "final hours", "winner", "prize", "deal", "cash", "earn", "easy money",
    "get rich quick", "credit", "debt", "refinance", "investment", "miracle",
    "secret", "scientifically proven", "weight loss", "congratulations",
    "offer expires", "apply now", "order now", "call now"
]

def remove_spam_words(text: str) -> str:
    """Remove common spam-trigger words in a conservative, case-insensitive way."""
    print(f"[remove_spam_words] Starting with text: {repr(text)}")
    if not isinstance(text, str):
        print("[remove_spam_words] Input not a string, returning as is.")
        return text
    cleaned = text
    for w in SPAM_WORDS:
        if re.search(rf"\b{re.escape(w)}\b", cleaned, flags=re.IGNORECASE):
            print(f"[remove_spam_words] Removing word: '{w}'")
        cleaned = re.sub(rf"\b{re.escape(w)}\b", "", cleaned, flags=re.IGNORECASE)
        print(f"[remove_spam_words] Text after removing '{w}': {repr(cleaned)}")
    # collapse extra whitespace created by removals
    # Collapse horizontal spaces but PRESERVE line breaks
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    # Trim spaces around newlines, keep \n and \n\n intact
    cleaned = re.sub(r"[ \t]*\n[ \t]*", "\n", cleaned)
    cleaned = cleaned.strip()
    print(f"[remove_spam_words] Final cleaned text: {repr(cleaned)}")
    return cleaned

def strip_bracketed(text: str) -> str:
    """
    Remove any content enclosed in [] (and the brackets themselves),
    even if there are multiple bracketed chunks. Then collapse extra spaces.
    """
    print(f"[strip_bracketed] Starting text: {repr(text)}")
    if not isinstance(text, str):
        print("[strip_bracketed] Input not a string, returning as is.")
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
            print(f"[strip_bracketed] Using pattern: {pat}")
            cleaned = re.sub(pat, "", cleaned)
        if cleaned != before:
            print(f"[strip_bracketed] Text after pass: {repr(cleaned)}")
        changed = (cleaned != before)

    # Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    print(f"[strip_bracketed] Final cleaned text: {repr(cleaned)}")
    return cleaned

def remove_brackets(text: str) -> str:
    """
    Remove substrings enclosed in square brackets [ ... ] without altering spacing,
    line breaks, or other formatting outside of the removed bracket content.
    """
    print(f"[remove_brackets] Starting text: {repr(text)}")
    if not isinstance(text, str):
        print("[remove_brackets] Input not a string, returning as is.")
        return text
    result = re.sub(r"\[[^\]]*\]", "", text)
    print(f"[remove_brackets] Final text: {repr(result)}")
    return result

def sanitize_email_fields(subject: str, body: str) -> Tuple[str, str]:
    """
    Sanitize both subject and body by removing bracketed content and scrubbing spam words.
    Returns (clean_subject, clean_body).
    """
    print(f"[sanitize_email_fields] Starting subject: {repr(subject)}")
    print(f"[sanitize_email_fields] Starting body: {repr(body)}")
    clean_subject = remove_brackets(subject)
    print(f"[sanitize_email_fields] Subject after remove_brackets: {repr(clean_subject)}")
    clean_body = remove_brackets(body)
    print(f"[sanitize_email_fields] Body after remove_brackets: {repr(clean_body)}")
    clean_subject = remove_spam_words(clean_subject)
    print(f"[sanitize_email_fields] Subject after remove_spam_words: {repr(clean_subject)}")
    clean_body = remove_spam_words(clean_body)
    print(f"[sanitize_email_fields] Body after remove_spam_words: {repr(clean_body)}")
    print(f"[sanitize_email_fields] Returning: ({repr(clean_subject)}, {repr(clean_body)})")
    return clean_subject, clean_body