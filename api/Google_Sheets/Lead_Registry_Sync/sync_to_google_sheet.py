import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import logging
from datetime import datetime
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# === LOGGING SETUP ===
log_file_path = os.path.join(os.path.dirname(__file__), 'logs', 'sync_log.txt')
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler(sys.stdout)
    ]
)

# === CONFIG ===
CSV_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/leads/Lead_Registry/leads_registry.csv"
CREDS_PATH = "/Users/kevinnovanta/backend_for_ai_agency/Creds/google_sheets_key.json"
SHEET_ID = "1xIxtFeaHNLteRKTsVJafbuswk1pOT12GW8TYCNUxW8U"  # ← Replace with your actual Google Sheet ID


def sync_leads_to_sheet():
    try:
        # === AUTH ===
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scope)
        client = gspread.authorize(creds)

        # === LOAD SHEET ===
        sheet = client.open_by_key(SHEET_ID).sheet1

        # === LOAD CSV AND PREPARE DATA ===
        import numpy as np
        df = pd.read_csv(CSV_PATH)
        df = df.replace({np.nan: ""})

        data = [df.columns.tolist()] + df.values.tolist()

        # === Only clear & update if there's valid data ===
        has_data = df.dropna(how="all").shape[0] > 0

        if has_data:
            logging.info(f"🔄 Syncing {len(data)-1} rows to Google Sheet...")
            sheet.update(range_name="A1", values=data)
            logging.info("✅ CSV successfully synced to Google Sheet.")
            print("✅ CSV successfully synced to Google Sheet.")
        else:
            logging.warning("⚠️ CSV is empty or only contains NaNs — skipping sync.")
            print("⚠️ CSV is empty or only contains NaNs — skipping sync.")
    except Exception as e:
        logging.exception("❌ Error syncing CSV to Google Sheet.")
        logging.error(f"❌ Error during sync: {str(e)}")
        print(f"❌ Sync failed: {e}")

# === AUTO-SYNC LOOP OR SINGLE RUN ===
if "--loop" in sys.argv:
    while True:
        sync_leads_to_sheet()
        logging.info("⏱️ Waiting 30 seconds for the next sync cycle...")
        print("⏱️ Waiting 30 seconds for the next sync cycle...")
        time.sleep(30)
else:
    sync_leads_to_sheet()

logging.shutdown()