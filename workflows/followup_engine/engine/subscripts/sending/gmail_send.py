"""Gmail sender with thread-link behavior.
If `thread_link` is provided, reply in that thread; if not, Gmail starts a new thread.

Requirements (install once):
    pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib

On first use per inbox (or if a token is revoked/expired), this will open a browser
window to complete OAuth and then cache the token JSON.
"""
from __future__ import annotations

import os
import base64
from email.message import EmailMessage
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# ==== User-specific defaults (can be overridden by env vars) ====
CREDENTIALS_PATH_DEFAULT = \
    "/Users/kevinnovanta/backend_for_ai_agency/Creds/credentials.json"
# Support a single shared token file *or* per-inbox token files.
TOKEN_FILE_DEFAULT = \
    "/Users/kevinnovanta/backend_for_ai_agency/Creds/token.json"
TOKENS_DIR_DEFAULT = \
    "/Users/kevinnovanta/backend_for_ai_agency/Creds/tokens"  # directory, not ".json"

# Env overrides
CREDENTIALS_PATH = os.environ.get("GMAIL_OAUTH_CLIENT_JSON", CREDENTIALS_PATH_DEFAULT)
TOKEN_FILE = os.environ.get("GMAIL_TOKEN_FILE", TOKEN_FILE_DEFAULT)
TOKENS_DIR = os.environ.get("GMAIL_TOKENS_DIR", TOKENS_DIR_DEFAULT)

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _token_path_for_inbox(inbox: str) -> str:
    # Store one token per inbox when using TOKENS_DIR
    safe = inbox.strip()
    return os.path.join(TOKENS_DIR, f"{safe}.json")


def _load_or_create_creds(inbox: str) -> Credentials:
    if not os.path.exists(CREDENTIALS_PATH):
        raise RuntimeError(
            f"Gmail OAuth client credentials not found at {CREDENTIALS_PATH}. "
            f"Set GMAIL_OAUTH_CLIENT_JSON or place credentials there."
        )

    # Decide which token path to prefer: single file or per-inbox
    preferred_token_path = TOKEN_FILE or _token_path_for_inbox(inbox)
    per_inbox_token_path = _token_path_for_inbox(inbox)

    token_path: Optional[str] = None
    creds: Optional[Credentials] = None

    # Try single token file first if it exists
    if preferred_token_path and os.path.exists(preferred_token_path):
        token_path = preferred_token_path
        print(f"[gmail_send] Using single token file at: {token_path}")
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception as e:
            print(f"[gmail_send] Failed to load token from {token_path}: {e}")
            creds = None

    # Otherwise try per-inbox token
    if not creds and os.path.exists(per_inbox_token_path):
        token_path = per_inbox_token_path
        print(f"[gmail_send] Using per-inbox token at: {token_path}")
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception as e:
            print(f"[gmail_send] Failed to load token from {token_path}: {e}")
            creds = None

    # If loaded creds have mismatched scopes, force re-auth
    if creds and creds.scopes and set(creds.scopes) != set(SCOPES):
        print(f"[gmail_send] Token scopes {creds.scopes} != required {SCOPES}; will re-auth.")
        creds = None

    # Try to refresh existing creds
    if creds and not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                print(f"[gmail_send] Refreshing token from {token_path}...")
                creds.refresh(Request())
                # Save refreshed token back to same path
                if token_path:
                    _ensure_parent_dir(token_path)
                    with open(token_path, "w", encoding="utf-8") as f:
                        f.write(creds.to_json())
                    print(f"[gmail_send] Token refreshed and saved: {token_path}")
            except Exception as e:
                print(f"[gmail_send] Token refresh failed: {e}")
                # Delete the bad token so we can re-auth cleanly
                if token_path and os.path.exists(token_path):
                    try:
                        os.remove(token_path)
                        print(f"[gmail_send] Deleted invalid token: {token_path}")
                    except Exception:
                        pass
                creds = None

    # If we still don't have valid creds, run the OAuth flow
    if not creds or not creds.valid:
        print("[gmail_send] Starting OAuth flow to obtain new token...")
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
        creds = flow.run_local_server(port=0)

        # Decide where to save: if single token file path is set, use that; else per-inbox
        token_path = preferred_token_path if preferred_token_path else per_inbox_token_path
        _ensure_parent_dir(token_path)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
        print(f"[gmail_send] Saved new token to: {token_path}")

    return creds


def _gmail_service(creds: Credentials):
    return build("gmail", "v1", credentials=creds)


def _rfc822(sender: str, to: str, subject: str, body: str) -> EmailMessage:
    msg = EmailMessage()
    msg["To"] = to
    msg["From"] = sender
    msg["Subject"] = subject
    msg.set_content(body)
    return msg


def _link_to_thread_id(link: Optional[str]) -> Optional[str]:
    """Extract Gmail threadId from a link like
    https://mail.google.com/mail/u/0/#inbox/<THREAD_ID>
    """
    if not link:
        return None
    try:
        frag = link.split("#", 1)[-1]  # e.g., inbox/<THREAD_ID>
        tid = frag.split("/")[-1]
        return tid if tid and tid.lower() != "inbox" else None
    except Exception:
        return None


def _thread_id_to_link(user_index: int, thread_id: str) -> str:
    return f"https://mail.google.com/mail/u/{user_index}/#inbox/{thread_id}"


def send_followup(*, inbox: str, to: str, subject: str, body: str, thread_link: str | None = None) -> Dict[str, Any]:
    """Send an email via Gmail API. If thread_link is present, reply in-thread.

    Returns a dict: {status, sent_at, thread_link, thread_id, bounce_status?, notes?}
    """
    print(
        f"send_followup called with inbox={inbox}, to={to}, subject={subject}, thread_link={thread_link}"
    )
    try:
        creds = _load_or_create_creds(inbox)
        service = _gmail_service(creds)

        msg = _rfc822(sender=inbox, to=to, subject=subject, body=body)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

        thread_id = _link_to_thread_id(thread_link) if thread_link else None
        body_payload: Dict[str, Any] = {"raw": raw}
        if thread_id:
            body_payload["threadId"] = thread_id
            print(f"[gmail_send] Reusing existing threadId: {thread_id}")

        sent = service.users().messages().send(userId="me", body=body_payload).execute()
        sent_at = datetime.now(timezone.utc).isoformat()

        final_tid = sent.get("threadId") or thread_id
        final_link = _thread_id_to_link(0, final_tid) if final_tid else None

        print(f"[gmail_send] Status=ok, sent_at={sent_at}, thread_id={final_tid}")
        return {
            "status": "ok",
            "sent_at": sent_at,
            "thread_link": final_link,
            "thread_id": final_tid,
            "bounce_status": None,
            "notes": "gmail_api"
        }

    except HttpError as e:
        print(f"[gmail_send] HttpError: {e}")
        return {"status": "error", "reason": "http_error", "detail": str(e)}
    except Exception as e:
        print(f"[gmail_send] Error: {e}")
        return {"status": "error", "reason": "exception", "detail": str(e)}