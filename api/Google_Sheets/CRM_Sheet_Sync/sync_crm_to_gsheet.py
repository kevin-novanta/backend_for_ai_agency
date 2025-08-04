import gspread
import pandas as pd
import time
from google.oauth2.service_account import Credentials
import logging

# Setup logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("/Users/kevinnovanta/backend_for_ai_agency/api/Google_Sheets/CRM_Sheet_Sync/logs/sync_output.log"),
        logging.StreamHandler()
    ]
)

# === CONFIG ===
CSV_PATH = "data/leads/CRM_Leads/CRM_leads.csv"
GOOGLE_SHEET_ID = "188Z8BYrbnt4Many31xXHs7H7FF2Crf0Zf10u7MvsFjo"
WORKSHEET_NAME = "Sheet1"
SYNC_INTERVAL = 30  # seconds
CREDENTIALS_PATH = "Creds/google_sheets_key.json"

def load_csv(csv_path):
    try:
        return pd.read_csv(csv_path)
    except Exception as e:
        logging.error(f"❌ Failed to load CSV: {e}")
        return pd.DataFrame()

def sync_to_gsheet():
    logging.info("🔁 Starting Google Sheet auto-sync...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)

    while True:
        try:
            df = load_csv(CSV_PATH)
            if df.empty:
                logging.info("⚠️ CSV is empty, skipping sync.")
                time.sleep(SYNC_INTERVAL)
                continue

            sheet = client.open_by_key(GOOGLE_SHEET_ID)
            worksheet = sheet.worksheet(WORKSHEET_NAME)

            # Clear current worksheet
            worksheet.clear()

            # Prepare data
            df = df.fillna("")  # Replace NaN with empty strings
            rows = [df.columns.values.tolist()] + df.values.tolist()
            worksheet.update(values=rows, range_name="A1")

            logging.info("✅ CSV successfully synced to Google Sheet.")
            logging.info(f"⏱️ Waiting {SYNC_INTERVAL} seconds for the next sync cycle...")
        except Exception as e:
            logging.error(f"❌ Sync error: {e}")

        time.sleep(SYNC_INTERVAL)

if __name__ == "__main__":
    sync_to_gsheet()