import pandas as pd
import os
import re

def extract_domain_name(url):
    """
    Mimics Google Sheets logic for extracting company name from URL:
    """
    if not isinstance(url, str) or not url.strip():
        print(f"extract_domain_name: input is empty or not a string: {url!r}")
        return ""

    # Extract the domain from the URL
    match = re.search(r"(?:https?://)?(?:www\.)?([^/]+)", url)
    if not match:
        print(f"extract_domain_name: regex did not match for: {url!r}")
        return ""

    domain = match.group(1)
    # Split domain by '.' and take the first part (e.g., 'example' from 'example.com')
    domain_parts = domain.split(".")
    if not domain_parts:
        print(f"extract_domain_name: no domain parts found in: {domain!r}")
        return ""
    core_name = domain_parts[0]
    # Replace dashes with spaces, collapse multiple dashes/spaces, capitalize words
    core_name = re.sub(r"-+", " ", core_name)
    core_name = core_name.strip()
    result = core_name.title()
    print(f"extract_domain_name: original={url!r}, domain={domain!r}, core_name={core_name!r}, result={result!r}")
    return result

def clean_company_names(input_csv_path, output_csv_path):
    """
    Reads a CSV file, dynamically finds URL and company name columns,
    cleans the company names using the URL, and writes the updated CSV.
    """
    df = pd.read_csv(input_csv_path)

    # Try to auto-detect the columns
    url_column = next((col for col in df.columns if "custom" in col.lower() or "url" in col.lower()), None)
    company_column = next((col for col in df.columns if "company" in col.lower()), None)

    print(f"🔍 Detected URL column: {url_column}")
    print(f"🔍 Detected Company Name column: {company_column}")

    if not url_column or not company_column:
        raise ValueError("Could not detect 'custom/url' or 'company' column names in the CSV.")

    df[company_column] = df[url_column].apply(extract_domain_name)
    df.to_csv(output_csv_path, index=False)
    print(f"✅ Cleaned company names and saved to: {output_csv_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("No input path provided, defaulting to test path...")
        input_path = "/Users/kevinnovanta/backend_for_ai_agency/data/exports/Google_Leads/Cleaned_Google_Maps_Data/enriched_data.csv"
    else:
        input_path = sys.argv[1]

    output_path = input_path  # Overwrite the original file
    clean_company_names(input_path, output_path)
