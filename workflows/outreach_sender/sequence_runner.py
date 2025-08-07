from workflows.outreach_sender.AI_Intergrations.opener_ai_writer import generate_email
from integrations.gmail_sender import send_email as gmail_send_email



import csv
import json
from datetime import datetime
from pathlib import Path


# Use actual Gmail send logic
def send_email(recipient_email, subject, body):
    success, sender_email = gmail_send_email(to=recipient_email, subject=subject, html=body)
    return success, sender_email

def run_opener_sequence():
    # Load config
    control_path = Path(__file__).parent / "opener_controls.json"
    with open(control_path, "r") as f:
        controls = json.load(f)

    allowed_days = controls["days_to_send"]
    start_hour = controls["start_hour"]
    end_hour = controls["end_hour"]
    daily_limit = controls["daily_send_limit"]
    per_inbox_limit = controls["per_inbox_limit"]

    # Time check
    now = datetime.now()
    weekday = now.strftime("%A")
    if weekday not in allowed_days:
        print("⛔ Not a sending day.")
        return
    if not (start_hour <= now.hour < end_hour):
        print("⛔ Outside sending window.")
        return

    # Prompt for client name
    client_name = input("🔍 Enter the client name to run outreach for: ").strip().lower()

    # Load leads
    crm_path = Path("/Users/kevinnovanta/backend_for_ai_agency/data/leads/CRM_Leads/CRM_leads_copy.csv")
    leads_to_send = []

    with open(crm_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            status = row.get("Messaging Status", "").strip().lower()
            row_client = row.get("Client", "").strip().lower()
            sequence_stage = row.get("Sequence Stage", "").strip().lower()
            responded = row.get("Responded?", "").strip().lower()
            if row_client != client_name:
                continue
            if sequence_stage:
                continue
            if responded == "yes":
                continue
            if status not in ("", "untouched", "new"):
                continue
            leads_to_send.append(row)
            if len(leads_to_send) >= daily_limit:
                break

    if not leads_to_send:
        print(f"⚠️ No leads found for client: '{client_name}'")
        return

    print(f"📬 Preparing to send {len(leads_to_send)} opener emails...")

    inbox_count = max(1, daily_limit // per_inbox_limit)

    # Simulate batching by inbox
    for i, lead in enumerate(leads_to_send):
        inbox_index = i % inbox_count
        email = lead.get("Email")
        personalized = generate_email({
            "company": lead.get("Company Name", ""),
            "email": lead.get("Email", ""),
            "overview": lead.get("Custom 2", "")
        })
        subject = personalized["subject"]
        body = personalized["body_html"]

        success, sender_used = send_email(email, subject, body)
        if success:
            print(f"✅ Sent to {email}")
            lead["Messaging Status"] = "Opener Sent"
            lead["Campaign Type"] = "Opener"
            lead["Sequence Stage"] = "Sent"
            lead["Lead Stage"] = "New"
            lead["Last Contacted Date"] = datetime.now().strftime("%Y-%m-%d")
            lead["Campaign Assigned"] = "1"
            lead["Outreach Channel"] = "Email"
            lead["Owner / Assigned To"] = sender_used
            # Placeholder for bounce/failure tracking
            lead["Bounce Status"] = ""
            # Log opener email content, time, and date
            lead["Opener Email"] = body
            lead["Opener Time Sent"] = datetime.now().strftime("%H:%M:%S")
            lead["Opener Date Sent"] = datetime.now().strftime("%Y-%m-%d")
        else:
            print(f"❌ Failed to send to {email}")
            # Placeholder to mark failure, can be updated later
            lead["Bounce Status"] = "Failed to send"

    # Reload full CRM data and update relevant rows
    with open(crm_path, "r", newline="", encoding="utf-8") as csvfile:
        csvfile_data = list(csv.DictReader(csvfile))

    # Reopen CRM for rewriting
    with open(crm_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = csvfile_data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in csvfile_data:
            if row.get("Client", "").strip().lower() == client_name and row.get("Messaging Status", "").strip().lower() in ("", "untouched", "new"):
                # Find the updated status in leads_to_send
                matching = next((lead for lead in leads_to_send if lead["Email"] == row["Email"]), None)
                if matching:
                    row["Messaging Status"] = matching.get("Messaging Status", row.get("Messaging Status", ""))
                    row["Campaign Type"] = matching.get("Campaign Type", row.get("Campaign Type", ""))
                    row["Sequence Stage"] = matching.get("Sequence Stage", row.get("Sequence Stage", ""))
                    row["Lead Stage"] = matching.get("Lead Stage", row.get("Lead Stage", ""))
                    row["Last Contacted Date"] = matching.get("Last Contacted Date", row.get("Last Contacted Date", ""))
                    row["Campaign Assigned"] = matching.get("Campaign Assigned", row.get("Campaign Assigned", ""))
                    row["Outreach Channel"] = matching.get("Outreach Channel", row.get("Outreach Channel", ""))
                    row["Owner / Assigned To"] = matching.get("Owner / Assigned To", row.get("Owner / Assigned To", ""))
                    if "Bounce Status" in matching:
                        row["Bounce Status"] = matching.get("Bounce Status", row.get("Bounce Status", ""))
                    # Update Opener Email, Time Sent, and Date Sent
                    row["Opener Email"] = matching.get("Opener Email", row.get("Opener Email", ""))
                    row["Opener Time Sent"] = matching.get("Opener Time Sent", row.get("Opener Time Sent", ""))
                    row["Opener Date Sent"] = matching.get("Opener Date Sent", row.get("Opener Date Sent", ""))
            writer.writerow(row)