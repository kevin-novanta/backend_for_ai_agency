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
    "Company Name", "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3",
    "Campaign Type", "Sequence Stage", "Messaging Status", "Responded?", "Replied Timestamp",
    "Qualified?", "Last Message Sent Timestamp", "Added To Retargeting Campaign?",
    "Retargeting Stage", "Retargeting Status", "Retargeting Responded?",
    "Retargetin Replied Time Stamp", "Last Message Sent Time Stamp", "Recycled?",
    "Lead Stage", "Last Contacted Date", "Campaign Assigned", "Outreach Channel",
    "Owner / Assigned To", "Notes"
]

UPDATABLE_COLUMNS = [
    "Client Name", "Email", "First Name", "Last Name", "Company Name",
    "Phone Number", "Address", "Custom 1", "Custom 2", "Custom 3"
]

assert CRM_COLUMNS[0] == "Copywriting Document Link", "First column must be Copywriting Document Link"

def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()

def save_csv(df, path):
    df.to_csv(path, index=False, quoting=1)  # quoting=1 ensures all non-numeric fields are quoted

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

        # Truncate crm_df to match registry_df length
        crm_df = crm_df[:len(registry_df)].copy()

        # Ensure enough rows in crm_df to accommodate registry data
        while len(crm_df) < len(registry_df):
            empty_row = pd.Series({col: "" for col in CRM_COLUMNS})
            crm_df = pd.concat([crm_df, pd.DataFrame([empty_row])], ignore_index=True)

        # Overwrite only non-empty values in the updatable columns
        for col in UPDATABLE_COLUMNS:
            if col in registry_df.columns:
                for i in range(len(registry_df)):
                    new_val = registry_df.at[i, col]
                    if pd.notna(new_val) and str(new_val).strip() != "":
                        crm_df.at[i, col] = new_val

        crm_df = crm_df.reindex(columns=CRM_COLUMNS)  # Ensure column order including Copywriting Document Link
        save_csv(crm_df, CRM_PATH)

    except Exception as e:
        print(f"❌ Error during sync: {e}")

if __name__ == "__main__":
    sync_registry_to_crm()