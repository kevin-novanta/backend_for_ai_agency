#!/bin/bash

# Kill any existing instances of the sync script
pkill -f sync_to_google_sheet.py

# Wait briefly to ensure the process is killed
sleep 2

# Start the sync script in the background with logging
nohup python3 /Users/kevinnovanta/backend_for_ai_agency/api/Google_Sheets/Lead_Registry_Sync/sync_to_google_sheet.py --loop >> /Users/kevinnovanta/backend_for_ai_agency/api/Google_Sheets/Lead_Registry_Sync/logs/sync_log.txt 2>&1 &