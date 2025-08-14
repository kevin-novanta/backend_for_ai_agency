from __future__ import annotations
from pathlib import Path
from typing import Dict, List

# Google API imports
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account


class GmailClient:
    """Light wrapper around an authenticated Gmail service for a given user.
    Kept for future expansion if you want helper methods; currently unused by the runner.
    """
    def __init__(self, service):
        self.svc = service

    def search_ids(self, q: str, max_results: int = 200) -> List[str]:
        res = self.svc.users().messages().list(userId="me", q=q, maxResults=max_results).execute()
        return [m["id"] for m in res.get("messages", [])]

    def get_headers(self, msg_id: str) -> Dict:
        return self.svc.users().messages().get(
            userId="me",
            id=msg_id,
            format="metadata",
            metadataHeaders=[
                "From",
                "To",
                "Delivered-To",
                "Subject",
                "Message-ID",
                "In-Reply-To",
                "References",
                "Date",
                "Auto-Submitted",
                "List-Id",
                "Precedence",
            ],
        ).execute()


# --- Repo root discovery (robust) ---

def _find_repo_root(start: Path) -> Path:
    """Walk up from `start` until we find a directory that contains `Creds/`.
    Falls back to a few parents if not found."""
    cur = start
    for p in [cur] + list(cur.parents):
        if (p / "Creds").exists():
            return p
        # Accept common repo markers as hints
        if (p / ".git").exists() and (p / "Creds").exists():
            return p
    # Fallback: go up three levels (…/backend_for_ai_agency)
    try:
        return start.parents[3]
    except IndexError:
        return start

# ===== OAuth paths & scopes =====
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
]

# Compute repo root and Creds directory robustly from this file location
REPO_ROOT = _find_repo_root(Path(__file__).resolve())
CREDS_DIR = REPO_ROOT / "Creds"
TOKENS_DIR = CREDS_DIR / "tokens"
TOKENS_DIR.mkdir(parents=True, exist_ok=True)

# Preferred Gmail-specific service account key (you created this)
SA_KEY_PATH = CREDS_DIR / "service_account_gmail.json"
# Fallback to generic name if the gmail-specific one isn't present
if not SA_KEY_PATH.exists():
    _fallback = CREDS_DIR / "service_account.json"
    if _fallback.exists():
        SA_KEY_PATH = _fallback


def _token_path_for(email: str) -> Path:
    # Create a filesystem-safe token filename per inbox
    safe = (
        email.strip().lower().replace("@", "_at_").replace("+", "_plus_").replace(".", "_")
    )
    return TOKENS_DIR / f"{safe}.json"



def gmail_service_for_user(inbox_email: str):
    """Build and return an authenticated Gmail API *service* for the given inbox
    using a Google Workspace Service Account with Domain‑Wide Delegation (DWD).

    Requirements:
      - Admin granted DWD to this service account's Client ID in Admin Console
      - service_account.json present at <repo>/Creds/service_account.json
      - SCOPES limited to least privilege (e.g., gmail.readonly)
    """
    if not SA_KEY_PATH.exists():
        raise FileNotFoundError(
            f"Missing service account key at {SA_KEY_PATH}. Place your Workspace service account JSON key there."
        )

    # Load the service account credentials
    sa_creds = service_account.Credentials.from_service_account_file(
        str(SA_KEY_PATH), scopes=SCOPES
    )

    # Impersonate the target user mailbox via Domain‑Wide Delegation
    delegated = sa_creds.with_subject(inbox_email)

    # Build and return the Gmail API service
    service = build("gmail", "v1", credentials=delegated)
    return service