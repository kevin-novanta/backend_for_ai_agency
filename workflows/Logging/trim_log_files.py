

import os

# Path to log files and their maximum allowed lines
LOG_CONFIG = {
    "/Users/kevinnovanta/backend_for_ai_agency/api/Google_Sheets/CRM_Sheet_Sync/logs/sync_output.log": 10000,
    "/Users/kevinnovanta/backend_for_ai_agency/api/Google_Sheets/Lead_Registry_Sync/logs/sync_log.txt": 10000,
    # Add more log paths and line limits as needed
}

def trim_log_file(path, max_lines):
    if not os.path.exists(path):
        print(f"❌ Log file not found: {path}")
        return

    with open(path, "r") as file:
        lines = file.readlines()

    if len(lines) > max_lines:
        with open(path, "w") as file:
            file.writelines(lines[-max_lines:])
        print(f"✅ Trimmed {path}: kept last {max_lines} lines.")
    else:
        print(f"ℹ️ {path} is within line limit ({len(lines)} lines).")

if __name__ == "__main__":
    for log_path, line_limit in LOG_CONFIG.items():
        trim_log_file(log_path, line_limit)