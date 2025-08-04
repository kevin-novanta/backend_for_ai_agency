#!/bin/bash

# Kill any existing instances of the CRM sync script
pkill -f sync_crm_to_gsheet.py

# Wait briefly to ensure the process is killed
sleep 2

# Start the CRM sync script in the background with logging
nohup python3 /Users/kevinnovanta/backend_for_ai_agency/api/Google_Sheets/CRM_Sheet_Sync/sync_crm_to_gsheet.py >> /Users/kevinnovanta/backend_for_ai_agency/api/Google_Sheets/CRM_Sheet_Sync/logs/sync_output.log 2>&1 &
