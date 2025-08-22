from __future__ import annotations
import os, base64, re
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple, List

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from engine.subscripts.utils.crm_helpers import get
from engine.subscripts.io.thread_links import link_to_thread_id, thread_id_to_link  # if you have it; or inline

# Search needs read scope
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

CREDENTIALS_PATH = os.environ.get(
    "GMAIL_OAUTH_CLIENT_JSON",
    "/Users/kevinnovanta/backend_for_ai_agency/Creds/credentials_for_thread_recovery.json"
)
TOKEN_FILE = os.environ.get(
    "GMAIL_TOKEN_FILE",
    "/Users/kevinnovanta/backend_for_ai_agency/Creds/token.json"
)
TOKENS_DIR = os.environ.get(
    "GMAIL_TOKENS_DIR",
    "/Users/kevinnovanta/backend_for_ai_agency/Creds/tokens"
)

def _ensure_parent(path: str):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def _token_path_for(inbox: str) -> str:
    return os.path.join(TOKENS_DIR, f"{inbox}.json")

def _load_creds(inbox: str) -> Credentials:
    if not os.path.exists(CREDENTIALS_PATH):
        raise RuntimeError(f"Missing Gmail credentials at {CREDENTIALS_PATH}")

    token_path = TOKEN_FILE if (TOKEN_FILE and os.path.exists(TOKEN_FILE)) else _token_path_for(inbox)
    creds = None
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception:
            creds = None
    if creds and not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_path, "w", encoding="utf-8") as f:
                    f.write(creds.to_json())
            except Exception:
                creds = None
        else:
            creds = None
    if not creds:
        # Re-auth (may prompt in browser once)
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
        creds = flow.run_local_server(port=0)
        _ensure_parent(token_path)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds

def _svc(creds: Credentials):
    return build("gmail", "v1", credentials=creds)

def _short_fingerprint(text: str, length: int = 40) -> Optional[str]:
    if not text:
        return None
    t = re.sub(r"\s+", " ", text).strip()
    return t[:length] if t else None

def _build_query(inbox: str, to_email: str, subject: Optional[str], since_date: Optional[str]) -> str:
    # Gmail search syntax: from:, to:, subject:, newer:, older:
    # newer_than:2d is also possible, but we’ll prefer absolute if you have opener date
    parts = [f"from:{inbox}", f"to:{to_email}"]
    if subject:
        # Quote subject to match exact phrase
        parts.append(f'subject:"{subject}"')
    if since_date:
        # since_date in YYYY/MM/DD
        parts.append(f"newer:{since_date}")
    return " ".join(parts)

def find_thread_by_signals(row: Dict[str, Any], fields: Dict[str, Any], inbox: str,
                           max_candidates: int = 10) -> Optional[Dict[str, str]]:
    """
    Try to recover the thread by searching the owner's inbox using CRM signals.
    Returns {thread_id, thread_link} or None.
    """
    can = fields.get("canonical", {})
    email_col   = can.get("email", "Email")
    subj_cols   = [ "Opener Subject Sent", "Opener Subject", "Opener Subject Line" ]
    body_cols   = [ "Opener Body Sent", "Opener Body" ]
    date_cols   = [ "Opener Date Sent", "Opener Time Sent" ]  # prefer date; time is secondary

    to_email = (get(row, email_col) or "").strip()
    opener_subject = None
    for c in subj_cols:
        v = get(row, c)
        if v:
            opener_subject = v.strip()
            break
    opener_body = None
    for c in body_cols:
        v = get(row, c)
        if v:
            opener_body = v
            break

    # Build a small fingerprint to confirm a candidate message
    body_fp = _short_fingerprint(opener_body, 40)

    # Opener date → YYYY/MM/DD for Gmail query (optional)
    opener_date = None
    for c in date_cols:
        v = get(row, c)
        if v:
            # Accept YYYY-MM-DD or mm/dd/yyyy; normalize to YYYY/MM/DD
            s = str(v).strip().replace("-", "/")
            parts = s.split("/")
            if len(parts[0]) == 4:
                opener_date = s  # already YYYY/MM/DD
            elif len(parts) == 3 and len(parts[2]) == 4:
                opener_date = f"{parts[2]}/{parts[0]}/{parts[1]}"
            break

    if not to_email:
        print("[thread_resolver] Missing recipient email — cannot search.")
        return None

    try:
        creds = _load_creds(inbox)
        service = _svc(creds)
        q = _build_query(inbox, to_email, opener_subject, opener_date)
        print(f"[thread_resolver] Searching inbox={inbox} with q='{q}'")
        resp = service.users().messages().list(userId="me", q=q, maxResults=max_candidates).execute()
        msgs = resp.get("messages", []) or []
        if not msgs:
            print("[thread_resolver] No messages matched query.")
            return None

        # Check message snippets for body fingerprint
        for m in msgs:
            mid = m["id"]
            mfull = service.users().messages().get(userId="me", id=mid, format="metadata").execute()
            # metadata has snippet; if you need body parts, request 'full'
            snippet = (mfull.get("snippet") or "").strip()
            tid = mfull.get("threadId")
            if body_fp and body_fp.lower() not in snippet.lower():
                # not a strong match, but keep as fallback if nothing else matches
                pass
            if tid:
                link = thread_id_to_link(0, tid)
                print(f"[thread_resolver] Candidate matched: threadId={tid}")
                return {"thread_id": tid, "thread_link": link}

        # Fallback: return most recent threadId even if snippet didn’t match
        tid = msgs[0].get("threadId") or service.users().messages().get(userId="me", id=msgs[0]["id"]).execute().get("threadId")
        if tid:
            link = thread_id_to_link(0, tid)
            print(f"[thread_resolver] Fallback selected recent threadId={tid}")
            return {"thread_id": tid, "thread_link": link}

    except HttpError as e:
        print(f"[thread_resolver] HttpError: {e}")
    except Exception as e:
        print(f"[thread_resolver] Error: {e}")

    return None