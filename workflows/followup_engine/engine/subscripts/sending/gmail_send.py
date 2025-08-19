"""Gmail send shim with thread-link behavior.
If thread_link is provided, reply in that thread; if not, simulate a new thread and return its link.
Replace the internals with your real Gmail API call as needed.
"""

import uuid
from engine.subscripts.utils.dates import now_iso


def send_followup(*, inbox: str, to: str, subject: str, body: str, thread_link: str | None = None) -> dict:
    print(f"send_followup called with inbox={inbox}, to={to}, subject={subject}, body={body}, thread_link={thread_link}")
    # TODO: integrate your real Gmail API call here.
    status = "ok"
    sent_at = now_iso()

    if thread_link:
        print("Using existing thread_link.")
        # Assume we stayed in the same thread
        new_link = thread_link
    else:
        print("No thread_link provided, creating a new one.")
        # Simulate creation of a new thread link
        new_link = f"https://mail.google.com/mail/u/0/#inbox/{uuid.uuid4()}"

    print(f"Thread link to use: {new_link}")
    print(f"Status: {status}, sent_at: {sent_at}")

    return {
        "status": status,
        "sent_at": sent_at,
        "thread_link": new_link,
        "bounce_status": None,
        "notes": "shim"
    }