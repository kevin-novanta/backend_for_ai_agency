from __future__ import annotations
from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parents[3]  # .../workflows/followup_engine
DEFAULT_CANDIDATES = [
    Path("/Users/kevinnovanta/backend_for_ai_agency/data/leads/CRM_Leads/CRM_Leads_copy.csv"),
]

def _pick_csv_path() -> Path:
    print("[load_crm] Picking CRM CSV path...")
    # Prefer explicit candidates
    for p in DEFAULT_CANDIDATES:
        print(f"[load_crm] Checking default path: {p}")
        if p.exists():
            print(f"[load_crm] Found default CRM file at {p}")
            return p

    # Fallback: look for any CSV in the repo root that smells like a CRM
    print(f"[load_crm] Checking project root for files containing 'crm'")
    csvs = list(ROOT.glob("*.csv"))
    ranked = sorted(
        csvs,
        key=lambda p: (0 if "crm" in p.name.lower() else 1,
                       0 if "tracker" in p.name.lower() else 1,
                       len(p.name)),
    )
    if ranked:
        print(f"[load_crm] Found CRM file in project root: {ranked[0]}")
        return ranked[0]

    print("[load_crm] No CRM file found in any expected location")
    raise FileNotFoundError(
        "CRM CSV not found. Place your CSV at one of these paths:\n"
        + "\n".join(str(p) for p in DEFAULT_CANDIDATES)
        + "\n—or put a CSV in the project root with 'crm' in the filename."
    )

def load_crm() -> tuple[list[dict], list[str], Path]:
    """
    Load the CRM CSV into memory.
    Returns: (rows, headers, csv_path)
    - rows: list of dicts (header → value)
    - headers: list of column names in original order
    - csv_path: Path to the file loaded
    """
    print("[load_crm] Starting to load CRM")
    csv_path = _pick_csv_path()
    print(f"[load_crm] Selected CSV path: {csv_path}")
    print("[load_crm] Opened CSV file, reading headers and rows")
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = [dict(r) for r in reader]
    print(f"[load_crm] Loaded {len(rows)} rows with {len(headers)} columns")
    print("[load_crm] Finished loading CRM")
    return rows, headers, csv_path