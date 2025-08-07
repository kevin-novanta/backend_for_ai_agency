import gspread
from google.oauth2.service_account import Credentials

# === CONFIG ===
GOOGLE_SHEET_NAME = "Dynamic Live-Leads Registry Data Sheet (Synced)"
WORKSHEET_NAME = "Sheet1"  # Change if your sheet tab is named differently
CREDENTIALS_PATH = "/Users/kevinnovanta/backend_for_ai_agency/Creds/google_sheets_key.json"

HEADERS = [
    "Client Name", "Email", "First Name", "Last Name", "Company Name",
    "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3"
]

def reset_sheet_headers():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)

    # Open sheet by name and worksheet
    sheet = client.open(GOOGLE_SHEET_NAME)
    worksheet = sheet.worksheet(WORKSHEET_NAME)

    # Clear all content
    worksheet.clear()

    # Add headers
    worksheet.append_row(HEADERS)

    print("✅ Lead Registry Sheet headers reset successfully.")

if __name__ == "__main__":
    reset_sheet_headers()