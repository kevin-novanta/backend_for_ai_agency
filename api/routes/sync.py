

from fastapi import APIRouter
import subprocess

router = APIRouter()

@router.get("/sync-crm-to-sheet")
def trigger_crm_sync():
    try:
        subprocess.Popen(["python", "api/Google_Sheets/CRM_Sheet_Sync/sync_crm_to_gsheet.py"])
        return {"status": "✅ Sync script started"}
    except Exception as e:
        return {"status": "❌ Failed to start sync", "error": str(e)}