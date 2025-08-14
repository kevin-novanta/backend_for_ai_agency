from .runtime.runner import run_loop
from .Adapters.creds_loader import load_senders

if __name__ == "__main__":
    inboxes = load_senders()
    if not inboxes:
        print("No inboxes found in Creds/email_accounts.json")
    run_loop(inboxes, interval_sec=60, jitter_sec=15)