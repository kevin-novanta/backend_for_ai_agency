import pandas as pd
import time
import os

# File paths
REGISTRY_PATH = "data/leads/Lead_Registry/leads_registry.csv"
CRM_PATH = "data/leads/CRM_Leads/CRM_leads.csv"
SYNC_INTERVAL = 30  # seconds

# Explicit full CRM column list
CRM_COLUMNS = [
    "Copywriting Document Link", "Client Name", "Email", "First Name", "Last Name",
    "Company Name", "Phone Number", "Address", "Website / Profile Link", "Offer", "Niche",
    "Campaign Type", "Sequence Stage", "Messaging Status", "Responded?", "Replied Timestamp",
    "Qualified?", "Last Message Sent Timestamp", "Added To Retargeting Campaign?",
    "Retargeting Stage", "Retargeting Status", "Retargeting Responded?",
    "Retargetin Replied Time Stamp", "Last Message Sent Time Stamp", "Recycled?",
    "Lead Stage", "Last Contacted Date", "Campaign Assigned", "Outreach Channel",
    "Owner / Assigned To", "Notes"
]

UPDATABLE_COLUMNS = [
    "Client Name", "Email", "First Name", "Last Name", "Company Name",
    "Phone Number", "Address", "Website / Profile Link", "Offer", "Niche"
]

assert CRM_COLUMNS[0] == "Copywriting Document Link", "First column must be Copywriting Document Link"

def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

def save_csv(df, path):
    df.to_csv(path, index=False)

def sync_registry_to_crm():
    try:
        registry_df = load_csv(REGISTRY_PATH)
        crm_df = load_csv(CRM_PATH)

        # Always ensure crm_df uses all CRM columns
        if crm_df.empty:
            print("🟡 CRM is empty. Initializing structure with full columns.")
            crm_df = pd.DataFrame(columns=CRM_COLUMNS)
        else:
            # Add any missing columns to crm_df (with empty values)
            for col in CRM_COLUMNS:
                if col not in crm_df.columns:
                    crm_df[col] = ""
            crm_df = crm_df[CRM_COLUMNS]

        # Ensure registry_df has the same number of columns as updatable columns
        registry_values = registry_df.values.tolist()
        new_rows = pd.DataFrame(columns=CRM_COLUMNS)

        for row in registry_values:
            padded_row = row + [""] * (len(UPDATABLE_COLUMNS) - len(row))
            row_dict = {col: val if col in UPDATABLE_COLUMNS else "" for col, val in zip(CRM_COLUMNS, [""] * len(CRM_COLUMNS))}
            for i, col in enumerate(UPDATABLE_COLUMNS):
                row_dict[col] = padded_row[i]
            new_rows = pd.concat([new_rows, pd.DataFrame([row_dict])], ignore_index=True)

        # Append new rows and preserve all other CRM fields
        print(f"✅ Adding {len(new_rows)} new leads without overwriting existing fields.")
        updated_df = pd.concat([crm_df, new_rows], ignore_index=True)
        updated_df = updated_df.reindex(columns=CRM_COLUMNS)  # Ensure column order including Copywriting Document Link
        save_csv(updated_df, CRM_PATH)

    except Exception as e:
        print(f"❌ Error during sync: {e}")

if __name__ == "__main__":
    sync_registry_to_crm()