import gspread
from google.oauth2.service_account import Credentials

# === CONFIG ===
GOOGLE_SHEET_NAME = "Lead CRM & Outreach Tracker"
WORKSHEET_NAME = "Sheet1"  # Change if needed
CREDENTIALS_PATH = "/Users/kevinnovanta/backend_for_ai_agency/Creds/google_sheets_key.json"

HEADERS = [
    "Copywriting Document Link", "Client Name", "Email", "First Name", "Last Name", "Company Name",
    "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3", "Campaign Type", "Sequence Stage",
    "Messaging Status", "Responded?", "Replied Timestamp", "Qualified?", "Last Message Sent Timestamp",
    "Added To Retargeting Campaign?", "Retargeting Stage", "Retargeting Status", "Retargeting Responded?",
    "Retargetin Replied Time Stamp", "Last Message Sent Time Stamp", "Recycled?", "Lead Stage",
    "Last Contacted Date", "Campaign Assigned", "Outreach Channel", "Owner / Assigned To", "Opener Email",
    "Opener Time Sent", "Opener Date Sent", "Follow Up 1 Email", "Follow Up 1 Time Sent", "Follow Up 1 Date Sent", 
    "Follow Up 2 Email", "Follow Up 2 Time Sent", "Follow Up 2 Date Sent", "Follow Up 3 Email", "Follow Up 3 Time Sent", 
    "Follow Up 3 Date Sent", "Follow Up 4 Email", "Follow Up 4 Time Sent", "Follow Up 4 Date Sent", "Follow Up 5 Email", 
    "Follow Up 5 Time Sent", "Follow Up 5 Date Sent", "Follow Up 6 Email", "Follow Up 6 Time Sent", "Follow Up 6 Date Sent", "Notes"
]

def reset_sheet_headers():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)

    sheet = client.open(GOOGLE_SHEET_NAME)
    worksheet = sheet.worksheet(WORKSHEET_NAME)

    worksheet.clear()
    worksheet.append_row(HEADERS)

    print("✅ CRM Sheet headers reset successfully.")

if __name__ == "__main__":
    reset_sheet_headers()