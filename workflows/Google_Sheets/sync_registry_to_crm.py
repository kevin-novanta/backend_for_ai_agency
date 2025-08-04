import pandas as pd
import os

# File paths
REGISTRY_PATH = "data/leads/Lead_Registry/leads_registry.csv"
CRM_PATH = "data/leads/CRM_Leads/CRM_leads.csv"

# Explicitly preserve all columns in the CRM structure
ALL_CRM_COLUMNS = [
    "Copywriting Document Link", "Client Name", "Email", "First Name", "Last Name",
    "Company Name", "Phone Number", "Address", "Website / Profile Link", "Offer", "Niche",
    "Campaign Type", "Sequence Stage", "Messaging Status", "Responded?", "Replied Timestamp",
    "Qualified?", "Last Message Sent Timestamp", "Added To Retargeting Campaign?",
    "Retargeting Stage", "Retargeting Status", "Retargeting Responded?",
    "Retargetin Replied Time Stamp", "Last Message Sent Time Stamp", "Recycled?",
    "Lead Stage", "Last Contacted Date", "Campaign Assigned", "Outreach Channel",
    "Owner / Assigned To", "Notes"
]

# Mapped registry columns by position
MAPPED_COLUMNS = [
    "Client Name", "Email", "First Name", "Last Name", "Company Name",
    "Phone Number", "Address", "Website / Profile Link", "Offer", "Niche"
]

def load_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path)
    # Always return DataFrame with all CRM columns if file is missing
    return pd.DataFrame(columns=ALL_CRM_COLUMNS)

def save_csv(df, path):
    df.to_csv(path, index=False)

def cross_map_registry_to_crm():
    try:
        registry_df = load_csv(REGISTRY_PATH)
        crm_df = load_csv(CRM_PATH)

        if crm_df.empty:
            print("🟡 CRM is empty. Initializing structure with full columns.")
            crm_df = pd.DataFrame(columns=ALL_CRM_COLUMNS)

        # Create blank DataFrame with all CRM columns
        new_rows = pd.DataFrame(columns=ALL_CRM_COLUMNS)

        # Map data into designated columns only
        for i, col in enumerate(registry_df.columns):
            if i < len(MAPPED_COLUMNS) and MAPPED_COLUMNS[i] in new_rows.columns:
                new_rows[MAPPED_COLUMNS[i]] = registry_df.iloc[:, i]

        # Append new rows and preserve all other CRM fields
        print(f"✅ Adding {len(new_rows)} new leads without overwriting existing fields.")
        updated_df = pd.concat([crm_df, new_rows], ignore_index=True)
        save_csv(updated_df, CRM_PATH)

    except Exception as e:
        print(f"❌ Error during mapping: {e}")

if __name__ == "__main__":
    cross_map_registry_to_crm()