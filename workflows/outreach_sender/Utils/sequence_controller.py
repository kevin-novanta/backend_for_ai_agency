import pandas as pd
from datetime import datetime, timedelta

# Load CRM
def load_crm(path):
    return pd.read_csv(path)

# Define sequences
SEQUENCE_RULES = {
    "Opener": [
        "Opener Sent",
        "Opener Follow-Up #1",
        "Opener Follow-Up #2",
        "Opener Follow-Up #3",
        "Opener Follow-Up #4"
    ],
    "VSL": [
        "VSL Sent",
        "VSL Follow-Up #1",
        "VSL Follow-Up #2",
        "VSL Follow-Up #3",
        "VSL Follow-Up #4"
    ]
}

def get_next_stage(current_stage, sequence_type):
    try:
        sequence = SEQUENCE_RULES[sequence_type]
        idx = sequence.index(current_stage)
        return sequence[idx + 1] if idx + 1 < len(sequence) else None
    except (KeyError, ValueError):
        return None

def should_send_next(row, sequence_type, delay_days=2):
    if row["Responded?"] == "Yes":
        return False
    last_contacted = pd.to_datetime(row.get("Last Contacted Date", ""), errors="coerce")
    if pd.isna(last_contacted):
        return True
    return (datetime.now() - last_contacted).days >= delay_days

def get_due_leads(crm_df, sequence_type):
    due_leads = []
    for _, row in crm_df.iterrows():
        if row.get("Sequence Stage") in SEQUENCE_RULES.get(sequence_type, []):
            if should_send_next(row, sequence_type):
                next_stage = get_next_stage(row["Sequence Stage"], sequence_type)
                if next_stage:
                    due_leads.append({
                        "Email": row["Email"],
                        "Company": row["Company Name"],
                        "Industry": row.get("Industry", ""),
                        "Offer": row.get("Offer", ""),
                        "Next Stage": next_stage,
                        "Row Index": row.name
                    })
    return due_leads
