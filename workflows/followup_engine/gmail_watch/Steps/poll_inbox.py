from __future__ import annotations
from typing import List, Any


def _build_query(inbox: str, lookback_minutes: int) -> str:
    # Gmail search only supports day granularity for newer_than, so use at least 1 day.
    days = max(1, (lookback_minutes // (24 * 60)) or 1)
    # Only inbox, exclude self-sent from that inbox
    return f"in:inbox newer_than:{days}d -from:{inbox}"


def poll_ids(gc: Any, inbox: str, since_epoch_ms: int, lookback_minutes: int = 1440) -> List[str]:
    """Return message IDs for candidate inbound messages for this inbox.

    We use a broad Gmail query and leave precise time filtering (since_ms) to the runner
    after classification (using message.internalDate).
    """
    user_id = "me"
    q = _build_query(inbox, lookback_minutes)
    print(f"[poll_inbox] Query={q}")

    ids: List[str] = []
    page_token = None

    while True:
        req = (
            gc.users()
            .messages()
            .list(
                userId=user_id,
                q=q,
                maxResults=100,
                pageToken=page_token,
                includeSpamTrash=False,
            )
        )
        res = req.execute()
        msgs = res.get("messages", [])
        print(f"[poll_inbox] Found {len(msgs)} messages")
        for m in msgs:
            ids.append(m["id"])
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return ids