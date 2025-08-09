

import json
import datetime

from workflows.outreach_sender.Email_Scripts.send_email import send_email

# === Load controls from JSON ===
def load_controls():
    with open("workflows/outreach_sender/Utils/opener_controls.json", "r") as f:
        return json.load(f)

# === Check if sending is allowed now ===
def is_sending_window_open(controls):
    if not controls.get("outreach_enabled", False):
        return False

    now = datetime.datetime.now()
    current_day = now.strftime("%a")
    current_time = now.strftime("%H:%M")

    if current_day not in controls["days_allowed"]:
        return False

    if not (controls["start_time"] <= current_time <= controls["end_time"]):
        return False

    return True

# === Placeholder for fetching leads ===
def fetch_untouched_leads():
    # Mock 200 leads
    return [{"email": f"user{i}@example.com", "company": f"Company {i}"} for i in range(200)]

# === Batch leads across inboxes ===
def batch_leads(leads, inbox_count, per_inbox_limit, daily_limit):
    leads_to_send = leads[:daily_limit]
    batches = [[] for _ in range(inbox_count)]

    for i, lead in enumerate(leads_to_send):
        inbox_index = i % inbox_count
        if len(batches[inbox_index]) < per_inbox_limit:
            batches[inbox_index].append(lead)

    return batches

# === Main runner ===
def main():
    controls = load_controls()

    if not is_sending_window_open(controls):
        print("❌ Not within sending window or sending disabled.")
        return

    print("✅ Sending window is open.")
    leads = fetch_untouched_leads()
    print(f"📥 Fetched {len(leads)} untouched leads.")

    inbox_count = controls["daily_limit"] // controls["per_inbox_limit"]
    batches = batch_leads(leads, inbox_count, controls["per_inbox_limit"], controls["daily_limit"])

    for i, batch in enumerate(batches):
        print(f"📤 Inbox {i+1}: {len(batch)} leads")
        for lead in batch:
            to_email = lead["email"]
            subject = "Quick idea to simplify your workflows"
            body = "<p>This is a placeholder email body generated via prompt logic.</p>"
            send_email(to_email, subject, body)

if __name__ == "__main__":
    main()