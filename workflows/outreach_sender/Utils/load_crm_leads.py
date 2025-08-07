

import csv

def load_crm_leads(csv_path):
    """
    Loads CRM leads from the given CSV and maps them into the format
    required for AI generation.
    """
    leads = []

    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            company = row.get("Company Name", "").strip()
            overview = row.get("Custom 2", "").strip()
            email = row.get("Email", "").strip()

            if company and overview and email:
                leads.append({
                    "company": company,
                    "overview": overview,
                    "email": email
                })

    return leads