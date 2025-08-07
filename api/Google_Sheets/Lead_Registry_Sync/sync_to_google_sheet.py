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

        # === CLEAR DATA ROWS BEFORE SYNC (PRESERVE HEADER) ===
        sheet.batch_clear(["A2:Z"])
        logging.info("🧨 Cleared data rows (A2:Z), header preserved.")
        time.sleep(2)

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
        logging.info(f"📊 Loaded {len(df)} rows from CSV")
        # Drop rows where all columns except "Copywriting Document Link" are NaN or empty
        df = df.dropna(how='all', subset=[col for col in df.columns if col != "Copywriting Document Link"])
        logging.info(f"🧹 Cleaned CSV: {len(df)} rows remain after dropping mostly-empty rows")
        # Ensure Status column is present
        if "Status" not in df.columns:
            df["Status"] = ""
        df = df.replace({np.nan: ""})
        df = df.map(lambda x: x[:49000] if isinstance(x, str) and len(x) > 49000 else x)

        # === CLEAN AND PREPARE FOR COMPARISON ===
        # Only keep CSV columns that exist in the Google Sheet, but always include "Email"
        valid_columns = existing_data[0] if existing_data else df.columns.tolist()
        required_columns = set(valid_columns) | {"Email"}
        df = df[[col for col in df.columns if col in required_columns]]

        # Clean and normalize Email column
        df["Email"] = df["Email"].astype(str).str.strip().str.lower()
        existing_df["Email"] = existing_df["Email"].astype(str).str.strip().str.lower()
        df = df[df["Email"] != ""]
        existing_df = existing_df[existing_df["Email"] != ""]

        logging.info(f"📄 Loaded {len(existing_df)} existing rows from Google Sheet")

        # Prepare mapping from Email to row for updates
        email_to_row = {
            row["Email"]: idx + 2
            for idx, row in existing_df.iterrows()
        }

        updates = []
        appends = []

        for _, csv_row in df.iterrows():
            email = str(csv_row.get("Email", "")).strip().lower()
            if not email:
                continue
            row_values = [str(csv_row.get(col, "")) for col in df.columns]
            if email in email_to_row:
                row_number = email_to_row[email]
                end_col_letter = chr(ord('A') + len(row_values) - 1)
                updates.append({
                    "range": f"A{row_number}:{end_col_letter}{row_number}",
                    "values": [row_values]
                })
            else:
                appends.append(row_values)

        logging.info(f"✏️ {len(updates)} rows to update, ➕ {len(appends)} rows to append")
        print(f"📊 CSV rows: {len(df)}, Sheet rows: {len(existing_df)}, To update: {len(updates)}, To append: {len(appends)}")

        if updates:
            sheet.batch_update(updates)
            time.sleep(1.5)
            logging.info("✅ Batch update complete.")
            print("✅ Batch update complete.")

        CHUNK_SIZE = 500
        for i in range(0, len(appends), CHUNK_SIZE):
            chunk = appends[i:i + CHUNK_SIZE]
            sheet.append_rows(chunk)
            time.sleep(1.5)
            logging.info(f"✅ Appended rows {i + 1} to {i + len(chunk)}")
            print(f"✅ Appended rows {i + 1} to {i + len(chunk)}")

        if not updates and not appends:
            logging.info("ℹ️ No updates or appends necessary.")
            print("ℹ️ No updates or appends necessary.")
    except Exception as e:
        logging.exception("❌ Error syncing CSV to Google Sheet.")
        logging.error(f"❌ Error during sync: {str(e)}")
        print(f"❌ Sync failed: {e}")

# === AUTO-SYNC LOOP OR SINGLE RUN ===
if "--loop" in sys.argv:
    while True:
        sync_leads_to_sheet()
        logging.info("⏱️ Waiting 90 seconds for the next sync cycle...")
        print("⏱️ Waiting 90 seconds for the next sync cycle...")
        time.sleep(90)
else:
    sync_leads_to_sheet()

logging.shutdown()