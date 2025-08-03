from fastapi import APIRouter
import pandas as pd
import os

router = APIRouter()

# Path to your cleaned Google Maps leads CSV
ENRICHED_CSV_PATH = os.path.join("data", "exports", "Google_Leads", "Cleaned_Google_Maps_Data", "enriched_data.csv")

@router.get("/leads/google-maps")
def get_google_maps_leads():
    if not os.path.exists(ENRICHED_CSV_PATH):
        return {"error": "File not found."}

    df = pd.read_csv(ENRICHED_CSV_PATH)
    print(f"[DEBUG] Loaded {len(df)} leads from {ENRICHED_CSV_PATH}")
    return df.to_dict(orient="records")

@router.get("/")
def get_all_leads():
    if not os.path.exists(ENRICHED_CSV_PATH):
        return {"error": "File not found."}

    df = pd.read_csv(ENRICHED_CSV_PATH)
    print(f"[DEBUG] Loaded {len(df)} total leads from {ENRICHED_CSV_PATH}")
    return df.to_dict(orient="records")