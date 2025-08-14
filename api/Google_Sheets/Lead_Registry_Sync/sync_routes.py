

from fastapi import APIRouter
import subprocess

router = APIRouter()

@router.post("/lead-registry")
def trigger_lead_registry_sync():
    try:
        result = subprocess.run(
            ["python3", "sync_to_google_sheet.py"],
            capture_output=True,
            text=True,
            cwd="api/Google_Sheets/Lead_Registry_Sync"
        )
        return {
            "status": "success" if result.returncode == 0 else "error",
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    except Exception as e:
        return {
            "status": "exception",
            "message": str(e)
        }