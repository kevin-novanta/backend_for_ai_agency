import gspread
import pandas as pd
import time
from google.oauth2.service_account import Credentials
import logging
from gspread.utils import rowcol_to_a1
import traceback

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
CSV_PATH = "/Users/kevinnovanta/backend_for_ai_agency/data/leads/CRM_Leads/CRM_leads.csv"
GOOGLE_SHEET_ID = "188Z8BYrbnt4Many31xXHs7H7FF2Crf0Zf10u7MvsFjo"
WORKSHEET_NAME = "Sheet1"
SYNC_INTERVAL = 90  # seconds
CREDENTIALS_PATH = "/Users/kevinnovanta/backend_for_ai_agency/Creds/google_sheets_key.json"

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
            logging.info(f"📊 Loaded {len(df)} rows from CSV")
            if df.empty:
                logging.info("⚠️ CSV is empty, skipping sync.")
                time.sleep(SYNC_INTERVAL)
                continue

            sheet = client.open_by_key(GOOGLE_SHEET_ID)
            worksheet = sheet.worksheet(WORKSHEET_NAME)

            # Clear columns B to K (2 to 11) except the header row
            worksheet.batch_clear(["L2:AZ"])
            logging.info("🧹 Cleared columns L@ to AZ (Logic Range) before syncing.")

            # Prepare data
            df = df.fillna("")  # Replace NaN with empty strings

            # Get existing data from worksheet
            existing_data = worksheet.get_all_values()
            if not existing_data:
                # If worksheet is empty, add header row
                worksheet.append_row(df.columns.values.tolist())
                existing_data = worksheet.get_all_values()

            header = existing_data[0]
            data_rows = existing_data[1:]
            logging.info(f"📄 Loaded {len(data_rows)} existing rows from Google Sheet")

            # Map header to column index
            header_index = {col: idx for idx, col in enumerate(header)}

            # Columns to update (aligned with Google Sheet target columns)
            update_columns = [
                "Campaign Type", "Sequence Stage", "Messaging Status", "Responded?", "Replied Timestamp", "Qualified?",
                "Last Message Sent Timestamp", "Added To Retargeting Campaign?", "Retargeting Stage", "Retargeting Status",
                "Retargeting Responded?", "Retargetin Replied Time Stamp", "Last Message Sent Time Stamp", "Recycled?",
                "Lead Stage", "Last Contacted Date", "Campaign Assigned", "Outreach Channel", "Owner / Assigned To",
                "Opener Email", "Opener Time Sent", "Opener Date Semt", "Follow Up 1 Email", "Follow Up 1 Time Sent",
                "Follow Up 1 Date Sent", "Follow Up 2 Email", "Follow Up 2 Time Sent", "Follow Up 2 Date Sent",
                "Follow Up 3 Email", "Follow Up 3 Time Sent", "Follow Up 3 Date Sent", "Follow Up 4 Email",
                "Follow Up 4 Time Sent", "Follow Up 4 Date Sent", "Follow Up 5 Email", "Follow Up 5 Time Sent",
                "Follow Up 5 Date Sent", "Follow Up 6 Email", "Follow Up 6 Time Sent", "Follow Up 6 Date Sent", "Notes"
            ]

            # Map email to row number in sheet (1-based, including header)
            email_to_row = {}
            for i, row in enumerate(data_rows, start=2):  # start=2 because header is row 1
                if len(row) > header_index.get("Email", -1):
                    email_to_row[row[header_index["Email"]].strip().lower()] = i

            updates = []
            appends = []

            for _, csv_row in df.iterrows():
                email = str(csv_row.get("Email", "")).strip().lower()
                if not email:
                    continue

                row_values = []
                for col_name in update_columns:
                    row_values.append(str(csv_row.get(col_name, "")))

                if email in email_to_row:
                    row_number = email_to_row[email]
                    start_col = 12  # Column L
                    end_col = 52    # Column AZ
                    cell_range = f"{rowcol_to_a1(row_number, start_col)}:{rowcol_to_a1(row_number, end_col)}"
                    updates.append({
                        "range": f"{WORKSHEET_NAME}!{cell_range}",
                        "values": [row_values]
                    })
                    # The original updates.append block is replaced above.
                else:
                    appends.append(row_values)

            logging.info(f"✏️ {len(updates)} rows to update, ➕ {len(appends)} rows to append")

            # Perform updates
            if updates:
                worksheet.batch_update([{
                    "range": u["range"],
                    "values": u["values"]
                } for u in updates])
                logging.info("✅ Batch update complete.")
                time.sleep(1.5)

            # Perform appends in chunks
            CHUNK_SIZE = 500
            for i in range(0, len(appends), CHUNK_SIZE):
                chunk = appends[i:i + CHUNK_SIZE]
                padded_chunk = [[""] * 11 + row for row in chunk]
                worksheet.append_rows(padded_chunk)
                logging.info(f"✅ Appended rows {i + 1} to {i + len(chunk)}")
                time.sleep(1.5)

            logging.info("✅ CSV successfully synced to Google Sheet.")
            logging.info(f"⏱️ Waiting {SYNC_INTERVAL} seconds for the next sync cycle...")
        except Exception as e:
            logging.error("❌ Sync error:\n" + traceback.format_exc())

        time.sleep(SYNC_INTERVAL)

if __name__ == "__main__":
    sync_to_gsheet()