from __future__ import annotations
import json
from pathlib import Path

DEFAULT_ACCOUNTS = Path("Creds/email_accounts.json")  # adjust if yours differs

def load_senders(path: Path | None = None) -> list[str]:
    p = Path(path or DEFAULT_ACCOUNTS)
    if not p.exists():
        return []
    data = json.loads(p.read_text())
    # expect e.g. {"accounts":[{"email":"info@..."}, ...]}
    if isinstance(data, dict) and "accounts" in data:
        return [a.get("email") for a in data["accounts"] if a.get("email")]
    if isinstance(data, list):
        return [a.get("email") for a in data if isinstance(a, dict) and a.get("email")]
    return []