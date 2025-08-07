import smtplib
import ssl
import json
import random
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# Load email accounts and limits
credentials_path = Path(__file__).parent.parent.parent / "creds" / "email_accounts.json"
with open(credentials_path, "r") as f:
    email_accounts = json.load(f)

# Track how many emails each inbox has sent today
sent_counts = {acc["email"]: 0 for acc in email_accounts}

# Set daily max per inbox
DAILY_LIMIT = 40

# Reset tracking if needed (new day)
today = datetime.now().strftime("%Y-%m-%d")
tracking_path = Path(__file__).parent / "email_send_tracking.json"

if tracking_path.exists():
    with open(tracking_path, "r") as f:
        tracking_data = json.load(f)
    if tracking_data.get("date") != today:
        tracking_data = {"date": today, "sent_counts": sent_counts}
else:
    tracking_data = {"date": today, "sent_counts": sent_counts}

# Update local reference
sent_counts.update(tracking_data["sent_counts"])

def get_available_sender():
    available_accounts = [
        acc for acc in email_accounts if sent_counts.get(acc["email"], 0) < DAILY_LIMIT
    ]
    if not available_accounts:
        raise Exception("All inboxes have reached the daily limit.")
    return random.choice(available_accounts)

def send_email(to_email, subject, body):
    sender = get_available_sender()
    sender_email = sender["email"]
    sender_password = sender["password"]
    smtp_server = sender.get("smtp_server", "smtp.gmail.com")
    smtp_port = sender.get("smtp_port", 587)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = to_email

    part = MIMEText(body, "html")
    msg.attach(part)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls(context=context)
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, msg.as_string())

        sent_counts[sender_email] += 1
        tracking_data["sent_counts"] = sent_counts
        with open(tracking_path, "w") as f:
            json.dump(tracking_data, f, indent=2)

        print(f"✅ Email sent from {sender_email} to {to_email}")
        return True, sender_email
    except Exception as e:
        print(f"❌ Failed to send email from {sender_email} to {to_email}: {e}")
        return False, None