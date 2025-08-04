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

        # === FETCH EXISTING SHEET DATA ===
        existing_data = sheet.get_all_values()
        if not existing_data:
            logging.warning("⚠️ Sheet is empty — skipping deletion check.")
            existing_df = pd.DataFrame()
        else:
            existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])

        # === LOAD CSV AND PREPARE DATA ===
        import numpy as np
        df = pd.read_csv(CSV_PATH)
        # Ensure Status column is present
        if "Status" not in df.columns:
            df["Status"] = ""
        df = df.replace({np.nan: ""})

        # === CLEAN AND PREPARE FOR COMPARISON ===
        # Only keep CSV columns that exist in the Google Sheet
        valid_columns = existing_data[0] if existing_data else df.columns.tolist()
        df = df[[col for col in df.columns if col in valid_columns]]

        df["Email"] = df["Email"].astype(str).str.strip()
        existing_df["Email"] = existing_df["Email"].astype(str).str.strip()

        df = df[df["Email"] != ""]
        existing_df = existing_df[existing_df["Email"] != ""]

        csv_emails = set(df["Email"])
        sheet_emails = set(existing_df["Email"])

        data = [df.columns.tolist()] + df.values.tolist()

        # === Only clear & update if there's valid data ===
        has_data = df.dropna(how="all").shape[0] > 0

        if has_data:
            logging.info(f"🔄 Syncing {len(data)-1} rows to Google Sheet...")
            # Determine the range to overwrite (e.g. A1:Z1000 depending on data size)
            end_col_letter = chr(ord('A') + len(data[0]) - 1)
            end_row = len(data)
            update_range = f"A1:{end_col_letter}{end_row}"

            # Clear values without affecting formatting
            cell_list = sheet.range(update_range)
            for cell in cell_list:
                cell.value = ''
            sheet.update_cells(cell_list)

            sheet.update(values=data, range_name=update_range)
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