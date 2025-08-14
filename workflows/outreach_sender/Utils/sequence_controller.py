import pandas as pd
from datetime import datetime, timedelta

# Load CRM
def load_crm(path):
    df = pd.read_csv(path)
    print(f"[DEBUG] Loaded CRM from {path} with {len(df)} rows")
    return df

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
        next_stage = sequence[idx + 1] if idx + 1 < len(sequence) else None
        print(f"[DEBUG] Current stage: {current_stage}, Sequence type: {sequence_type}, Next stage: {next_stage}")
        return next_stage
    except (KeyError, ValueError):
        print(f"[DEBUG] Current stage: {current_stage}, Sequence type: {sequence_type}, Next stage: None")
        return None

def should_send_next(row, sequence_type, delay_days=2):
    print(f"[DEBUG] Checking if lead {row.get('Email', 'N/A')} is due for next stage in sequence {sequence_type}")
    if row["Responded?"] == "Yes":
        print(f"[DEBUG] Lead {row.get('Email', 'N/A')} already responded. Skipping.")
        return False
    last_contacted = pd.to_datetime(row.get("Last Contacted Date", ""), errors="coerce")
    if pd.isna(last_contacted):
        print(f"[DEBUG] Lead {row.get('Email', 'N/A')} has no last contact date. Due immediately.")
        return True
    days_passed = (datetime.now() - last_contacted).days
    if days_passed >= delay_days:
        print(f"[DEBUG] {days_passed} days since last contact. Lead is due.")
        return True
    else:
        print(f"[DEBUG] Only {days_passed} days since last contact. Not due yet.")
        return False

def get_due_leads(crm_df, sequence_type):
    print(f"[DEBUG] Getting due leads for sequence type: {sequence_type}")
    due_leads = []
    for _, row in crm_df.iterrows():
        if row.get("Sequence Stage") in SEQUENCE_RULES.get(sequence_type, []):
            if should_send_next(row, sequence_type):
                next_stage = get_next_stage(row["Sequence Stage"], sequence_type)
                if next_stage:
                    print(f"[DEBUG] Lead {row.get('Email', 'N/A')} is due for next stage: {next_stage}")
                    due_leads.append({
                        "Email": row["Email"],
                        "Company": row["Company Name"],
                        "Industry": row.get("Industry", ""),
                        "Offer": row.get("Offer", ""),
                        "Next Stage": next_stage,
                        "Row Index": row.name
                    })
                else:
                    print(f"[DEBUG] Lead {row.get('Email', 'N/A')} not due or no next stage.")
            else:
                print(f"[DEBUG] Lead {row.get('Email', 'N/A')} not due or no next stage.")
        else:
            print(f"[DEBUG] Lead {row.get('Email', 'N/A')} not due or no next stage.")
    return due_leads
