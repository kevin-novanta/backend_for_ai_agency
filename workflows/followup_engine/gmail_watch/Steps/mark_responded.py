from __future__ import annotations
import csv
import os

print("Starting mark_responded script...")

# Try package-relative import first (works when run as a package)
try:
    print("Trying package-relative import of StateStore...")
    from ...utils.state_store import StateStore  # workflows.followup_engine.utils.state_store
except Exception:  # pragma: no cover
    # Fallback: absolute import if editor/runtime layout differs
    try:
        print("Trying absolute import of StateStore...")
        from workflows.followup_engine.utils.state_store import StateStore
    except Exception:
        print("StateStore import failed; will no-op if not available.")
        StateStore = None  # we will no-op if not available

# Use the canonical CRM CSV path
CRM_CSV_PATH = \
    "/Users/kevinnovanta/backend_for_ai_agency/data/leads/CRM_Leads/CRM_leads_copy.csv"

def mark_yes(lead_email: str, subject: str, date_iso: str) -> bool:
    """Update CSV (Responded?=Yes, Last Inbound Timestamp, Stop Reason) and set StateStore to REPLIED.
    Returns True if the row was found and updated; False otherwise.
    """
    print(f"mark_yes called with lead_email={lead_email}, subject={subject}, date_iso={date_iso}")

    from datetime import datetime, timezone
    if not date_iso:
        date_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        print(f"No date_iso provided; using current UTC {date_iso}")

    if not os.path.exists(CRM_CSV_PATH):
        print(f"CRM CSV file not found at {CRM_CSV_PATH}")
        return False
    print(f"CRM CSV file found at {CRM_CSV_PATH}")

    target = (lead_email or "").strip().lower()
    if not target:
        print("Empty lead_email provided to mark_yes")
        return False
    print(f"Processing lead: {target}")

    updated = False
    rows = []

    try:
        print("Opening CRM CSV file for reading...")
        with open(CRM_CSV_PATH, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = list(reader.fieldnames or [])
            if not fieldnames:
                print("CSV file has no headers.")
                return False
            print(f"CSV headers found: {fieldnames}")

            # Determine email column (case-insensitive)
            lower_map = {fn.lower(): fn for fn in fieldnames}
            email_key = lower_map.get("email") or lower_map.get("email address") or lower_map.get("e-mail")
            if not email_key:
                print("Could not find an email column in CRM CSV.")
                return False
            print(f"Email column identified as: {email_key}")

            # Ensure required columns exist in header
            for needed in ["Responded?", "Last Inbound Timestamp", "Replied Timestamp", "Stop Reason"]:
                if needed not in fieldnames:
                    print(f"Adding missing column to headers: {needed}")
                    fieldnames.append(needed)

            for row in reader:
                cur_email = (row.get(email_key) or "").strip().lower()
                if cur_email == target:
                    print(f"Found matching lead row for {target}, updating fields...")
                    row["Responded?"] = "Yes"
                    row["Last Inbound Timestamp"] = date_iso
                    row["Replied Timestamp"] = date_iso
                    row["Stop Reason"] = "REPLIED"
                    updated = True
                rows.append(row)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return False

    if not updated:
        print(f"Lead email {lead_email} not found in CRM CSV.")
        return False

    try:
        print("Saving updated lead state to CSV file...")
        with open(CRM_CSV_PATH, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(f"Error writing CSV file: {e}")
        return False

    try:
        print("Initializing StateStore...")
        if StateStore is not None:
            print(f"Setting StateStore status for {lead_email} to REPLIED...")
            StateStore.set_status(lead_email, "REPLIED")
        else:
            print("StateStore not available; skipping status update.")
    except Exception as e:
        print(f"Error setting StateStore status: {e}")
        # Do not fail the write if StateStore update fails

    print("mark_responded script completed successfully.")
    return True

def mark_no(lead_email: str, date_iso: str | None = None) -> bool:
    """Update CSV (Responded?=No) and set StateStore to NO (non-blocking).
    Returns True if the row was found and updated; False otherwise.
    """
    print(f"mark_no called with lead_email={lead_email}, date_iso={date_iso}")

    if not os.path.exists(CRM_CSV_PATH):
        print(f"CRM CSV file not found at {CRM_CSV_PATH}")
        return False
    print(f"CRM CSV file found at {CRM_CSV_PATH}")

    target = (lead_email or "").strip().lower()
    if not target:
        print("Empty lead_email provided to mark_no")
        return False
    print(f"Processing lead for NO: {target}")

    updated = False
    rows = []

    try:
        print("Opening CRM CSV file for reading (mark_no)...")
        with open(CRM_CSV_PATH, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            fieldnames = list(reader.fieldnames or [])
            if not fieldnames:
                print("CSV file has no headers.")
                return False
            print(f"CSV headers found (mark_no): {fieldnames}")

            # Determine email column (case-insensitive)
            lower_map = {fn.lower(): fn for fn in fieldnames}
            email_key = lower_map.get("email") or lower_map.get("email address") or lower_map.get("e-mail")
            if not email_key:
                print("Could not find an email column in CRM CSV.")
                return False
            print(f"Email column identified as (mark_no): {email_key}")

            # Ensure required columns exist in header (at least Responded?)
            for needed in ["Responded?", "Last Inbound Timestamp", "Replied Timestamp", "Stop Reason"]:
                if needed not in fieldnames:
                    print(f"Adding missing column to headers (mark_no): {needed}")
                    fieldnames.append(needed)

            for row in reader:
                cur_email = (row.get(email_key) or "").strip().lower()
                if cur_email == target:
                    print(f"Found matching lead row for {target}, setting Responded?=No...")
                    row["Responded?"] = "No"
                    # For NO we usually leave timestamp/stop reason blank to keep sequence eligible
                    row["Last Inbound Timestamp"] = date_iso or row.get("Last Inbound Timestamp", "")
                    # Keep Stop Reason empty so it doesn't block sequences
                    if row.get("Stop Reason") == "REPLIED":
                        print("Clearing Stop Reason since status is NO")
                        row["Stop Reason"] = ""
                    updated = True
                rows.append(row)
    except Exception as e:
        print(f"Error reading CSV file (mark_no): {e}")
        return False

    if not updated:
        print(f"Lead email {lead_email} not found in CRM CSV (mark_no).")
        return False

    try:
        print("Saving updated lead state to CSV file (mark_no)...")
        with open(CRM_CSV_PATH, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        print(f"Error writing CSV file (mark_no): {e}")
        return False

    try:
        print("Initializing StateStore (mark_no)...")
        if StateStore is not None:
            print(f"Setting StateStore status for {lead_email} to NO...")
            StateStore.set_status(lead_email, "NO")
        else:
            print("StateStore not available; skipping status update (mark_no).")
    except Exception as e:
        print(f"Error setting StateStore status (mark_no): {e}")
        # Do not fail the write if StateStore update fails

    print("mark_no completed successfully.")
    return True