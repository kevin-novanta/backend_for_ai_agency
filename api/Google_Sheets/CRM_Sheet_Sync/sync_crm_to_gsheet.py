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

            # Clear columns B to K (2 to 11) except the header row
            worksheet.batch_clear(["B2:K"])
            logging.info("🧹 Cleared columns B to K (Client Info Range) before syncing.")

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

            # Map header to column index
            header_index = {col: idx for idx, col in enumerate(header)}

            # Columns to update
            update_columns = ["Email", "First Name", "Last Name", "Company Name", "Phone Number", "Address", "Website / Profile Link", "Offer", "Niche"]

            # Map email to row number in sheet (1-based, including header)
            email_to_row = {}
            for i, row in enumerate(data_rows, start=2):  # start=2 because header is row 1
                if len(row) > header_index.get("Email", -1):
                    email_to_row[row[header_index["Email"]].strip().lower()] = i

            for _, csv_row in df.iterrows():
                email = str(csv_row.get("Email", "")).strip().lower()
                if not email:
                    continue  # skip rows without email

                if email in email_to_row:
                    # Update existing row
                    row_number = email_to_row[email]
                    for col_name in update_columns:
                        if col_name in header:
                            col_index = header_index[col_name] + 1  # gspread is 1-indexed for columns
                            value = str(csv_row.get(col_name, ""))
                            worksheet.update_cell(row_number, col_index, value)
                else:
                    # Append new row
                    new_row = [""] * len(header)
                    for col_name in update_columns:
                        if col_name in header:
                            idx = header_index[col_name]
                            new_row[idx] = str(csv_row.get(col_name, ""))
                    worksheet.append_row(new_row)

            logging.info("✅ CSV successfully synced to Google Sheet.")
            logging.info(f"⏱️ Waiting {SYNC_INTERVAL} seconds for the next sync cycle...")
        except Exception as e:
            logging.error(f"❌ Sync error: {e}")

        time.sleep(SYNC_INTERVAL)

if __name__ == "__main__":
    sync_to_gsheet()