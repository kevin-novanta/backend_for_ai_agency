import pandas as pd
import re
import os
import sys

TARGET_COLUMNS = [
    "Client",
    "Email",
    "First Name",
    "Last Name",
    "Company Name",
    "Phone Number",
    "Address",
    "Custom 1",
    "Custom 2",
    "Custom 3",
]

NON_B2B_EMAIL_KEYWORDS = [
    "gmail.com", "yahoo.com", "hotmail.com", "aol.com", "outlook.com", "icloud.com", "protonmail.com"
]
NON_B2B_COMPANY_KEYWORDS = [
    "student", "university", "school", "college", "gmail", "hotmail", "yahoo", "test", "personal"
]

def normalize_column_names(df):
    # Lowercase and strip columns
    df.columns = [col.strip().lower() for col in df.columns]
    # Mapping from possible variants to target columns
    col_map = {
        "email": "Email",
        "e-mail": "Email",
        "mail": "Email",
        "first name": "First Name",
        "firstname": "First Name",
        "last name": "Last Name",
        "lastname": "Last Name",
        "company": "Company Name",
        "company name": "Company Name",
        "organization": "Company Name",
        "org": "Company Name",
        "phone": "Phone Number",
        "phone number": "Phone Number",
        "mobile": "Phone Number",
        "address": "Address",
        "street": "Address",
        "custom 1": "Custom 1",
        "custom1": "Custom 1",
        "custom 2": "Custom 2",
        "custom2": "Custom 2",
        "custom 3": "Custom 3",
        "custom3": "Custom 3",
    }
    new_columns = {}
    for col in df.columns:
        key = col
        if key in col_map:
            new_columns[col] = col_map[key]
        else:
            # Try fuzzy matches
            for k in col_map:
                if k.replace(" ", "") == key.replace(" ", ""):
                    new_columns[col] = col_map[k]
                    break
    df = df.rename(columns=new_columns)
    # Add missing columns as empty
    for target_col in TARGET_COLUMNS:
        if target_col not in df.columns:
            df[target_col] = ""
    # Reorder columns
    df = df[TARGET_COLUMNS]
    return df

def is_valid_email(email):
    if not isinstance(email, str):
        return False
    email = email.strip()
    return re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", email) is not None

def is_non_b2b_lead(row):
    email = str(row.get("Email", "")).lower()
    company = str(row.get("Company Name", "")).lower()
    if any(kw in email for kw in NON_B2B_EMAIL_KEYWORDS):
        return True
    if any(kw in company for kw in NON_B2B_COMPANY_KEYWORDS):
        return True
    return False

def score_lead(row):
    # Heuristic scoring: +2 for company name, +2 for phone, +2 for address, +2 for first+last name, +2 for business email
    score = 0
    email = str(row.get("Email", "")).lower()
    company = str(row.get("Company Name", ""))
    if company and len(company.strip()) > 2:
        score += 2
    if row.get("Phone Number", "") and len(str(row.get("Phone Number", "")).strip()) > 5:
        score += 2
    if row.get("Address", "") and len(str(row.get("Address", "")).strip()) > 5:
        score += 2
    if str(row.get("First Name", "")).strip() and str(row.get("Last Name", "")).strip():
        score += 2
    # Business email
    if not any(kw in email for kw in NON_B2B_EMAIL_KEYWORDS):
        score += 2
    return score

def parse_and_format_leads(raw_csv_path, client_name):
    df = pd.read_csv(raw_csv_path)
    df = normalize_column_names(df)
    df["Client"] = client_name
    # --- Clean and format Custom 1, 2, 3 columns ---
    # Custom 1: Keep only valid URLs
    df["Custom 1"] = df["Custom 1"].apply(lambda x: x if isinstance(x, str) and x.startswith("http") else "")

    # Custom 2: Preserve full description, just strip whitespace if it's a string
    df["Custom 2"] = df["Custom 2"].apply(lambda x: x.strip() if isinstance(x, str) else "")

    # Custom 3: Extract keyword-like industry/niche
    import string
    def extract_industry(text):
        if not isinstance(text, str):
            return ""
        # Convert to lowercase, remove punctuation
        text = text.lower().translate(str.maketrans('', '', string.punctuation))
        # Tokenize
        tokens = text.split()
        # Pick the first 1-2 keywords that are not generic
        blacklist = {"services", "solutions", "llc", "inc", "company", "business"}
        keywords = [t for t in tokens if t not in blacklist and len(t) > 2]
        return keywords[0] if keywords else ""

    df["Custom 3"] = df["Custom 3"].apply(extract_industry)
    # Filter out rows without valid emails
    df = df[df["Email"].apply(is_valid_email)]
    # Drop non-B2B leads
    df = df[~df.apply(is_non_b2b_lead, axis=1)]
    # Score and filter
    df["score"] = df.apply(score_lead, axis=1)
    df = df[df["score"] >= 8]
    df = df.drop(columns=["score"])
    # Clean company names (removed as requested)
    # Deduplicate
    df.drop_duplicates(inplace=True)
    # Append to registry
    registry_path = "/Users/kevinnovanta/backend_for_ai_agency/data/leads/Lead_Registry/leads_registry.csv"
    if os.path.exists(registry_path):
        try:
            registry_df = pd.read_csv(registry_path)
            if registry_df.empty:
                raise ValueError("Empty registry file.")
            registry_df = normalize_column_names(registry_df)
        except (pd.errors.EmptyDataError, ValueError):
            registry_df = pd.DataFrame(columns=TARGET_COLUMNS)
    else:
        registry_df = pd.DataFrame(columns=TARGET_COLUMNS)

    combined_df = pd.concat([registry_df, df], ignore_index=True)
    combined_df = combined_df.fillna("")
    combined_df.drop_duplicates(subset=["Email", "Company Name", "Custom 1"], inplace=True)
    new_leads_count = len(combined_df) - len(registry_df)
    # Ensure JSON-compliant DataFrame by enforcing empty strings
    combined_df = combined_df.astype(str).replace("nan", "")
    combined_df.to_csv(registry_path, index=False)
    return df, new_leads_count

if __name__ == "__main__":
    client_name = os.environ.get("CLIENT_NAME")
    if not client_name:
        raise ValueError("Client name must be provided as an environment variable 'CLIENT_NAME'.")
    raw_csv_path = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Cleaned_Google_Maps_Data/enriched_data.csv"
    cleaned_df, added_count = parse_and_format_leads(raw_csv_path, client_name)
    print(f"✅ Saved {len(cleaned_df)} deduplicated and formatted leads to leads_registry.csv.")