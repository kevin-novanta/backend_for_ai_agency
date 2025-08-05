import os
import base64
from email.mime.text import MIMEText
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

TOKEN_PATH = "Creds/token.json"
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def get_gmail_service():
    creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    return service

def create_message(to_email, subject, body_html):
    message = MIMEText(body_html, 'html')
    message['to'] = to_email
    message['subject'] = subject
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw}


def send_email(to_email, subject, body_html):
    try:
        service = get_gmail_service()
        message = create_message(to_email, subject, body_html)
        sent = service.users().messages().send(userId="me", body=message).execute()
        print(f"✅ Email sent to {to_email} (ID: {sent['id']})")
        return sent
    except Exception as e:
        print(f"❌ Failed to send email to {to_email}: {e}")
        return None


# CLI interface for sending email interactively
if __name__ == "__main__":
    print("📤 Gmail Sender Interactive CLI")
    to_email = input("Enter recipient email: ").strip()
    subject = input("Enter subject: ").strip()

    print("\nEnter your email message. Press Shift+S on a new line when finished:")
    lines = []
    while True:
        line = input()
        if line == "S":  # Shift+S
            break
        lines.append(line)
    body_html = "<br>".join(lines)

    confirm = input(f"\nConfirm sending email to {to_email}? (y/n): ").strip().lower()
    if confirm == "y":
        send_email(to_email, subject, body_html)
    else:
        print("❌ Email send canceled.")
