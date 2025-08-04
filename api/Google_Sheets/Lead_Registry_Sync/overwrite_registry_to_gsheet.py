import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

import os
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from shared.logger import setup_logger

# Setup logger
logger = setup_logger(log_file_path="api/Google_Sheets/Lead_Registry_Sync/logs/sync_output.log")

# Google Sheets setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "Creds/google_sheets_key.json"
SHEET_NAME = "Dynamic Live-Leads Registry Data Sheet (Synced)"  # update to your actual sheet name
WORKSHEET_NAME = "Sheet1"      # or the correct tab name

def overwrite_sheet_with_csv():
    try:
        # Load CSV
        csv_path = "data/leads/Lead_Registry/leads_registry.csv"
        df = pd.read_csv(csv_path)

        # Authenticate and open the sheet
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open(SHEET_NAME)
        worksheet = sh.worksheet(WORKSHEET_NAME)

        # Clear all existing values from row 2 downward (but not formatting or headers)
        worksheet.batch_clear(["A2:Z"])
        logger.info("🧹 Sheet cleared successfully before overwrite.")

        # Re-upload entire data starting from A1 (preserves headers properly)
        values = [df.columns.tolist()] + df.values.tolist()
        worksheet.update("A1", values)
        logger.info("✅ Sheet data fully overwritten, including headers.")

        logger.info("✅ Successfully overwrote Leads Registry Google Sheet.")
        print("✅ Sheet overwritten successfully.")
    
    except Exception as e:
        logger.error(f"❌ Error overwriting sheet: {str(e)}")
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    overwrite_sheet_with_csv()