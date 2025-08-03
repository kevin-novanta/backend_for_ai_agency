import os
import subprocess
import sys

# Paths
ROOT_DIR = "/Users/kevinnovanta/backend_for_ai_agency"
RAW_CSV_PATH = os.path.join(ROOT_DIR, "data", "exports", "Cleaned_Google_Maps_Data", "enriched_data.csv")
CLEANER_SCRIPT = os.path.join(ROOT_DIR, "workflows", "lead_scraper", "utils", "company_name_cleaner", "company_name_cleaner.py")
DEDUPLICATOR_SCRIPT = os.path.join(ROOT_DIR, "workflows", "lead_scraper", "utils", "Deduplication", "deduplicator.py")
PARSER_SCRIPT = os.path.join(ROOT_DIR, "workflows", "lead_scraper", "utils", "parse_and_format_leads", "parse_and_format_leads.py")

def run_script(script_path, args=None):
    script_name = os.path.basename(script_path)
    print(f"\n🔧 Starting script: {script_name}")
    cmd = [sys.executable, script_path]
    if args:
        cmd.extend(args)
        print(f"   ➤ With arguments: {args}")
    else:
        print("   ➤ No arguments provided.")

    result = subprocess.run(cmd, capture_output=True, text=True)
    print("   ➤ STDOUT:")
    print(result.stdout)
    if result.stderr:
        print("   ⚠️ STDERR:")
        print(result.stderr)
        print(f"❌ Script {script_name} failed.\n")
    else:
        print(f"✅ Script {script_name} completed successfully.\n")

def main():
    print("🚀 Starting full lead processing pipeline...\n")
    if not os.path.exists(RAW_CSV_PATH):
        print(f"❌ Raw scraped CSV not found at: {RAW_CSV_PATH}")
        return

    print("🔹 Step 1: Running Company Name Cleaner...")
    run_script(CLEANER_SCRIPT, [RAW_CSV_PATH])
    print("✅ Step 1 completed successfully.\n")

    print("🔹 Step 2: Running Deduplication Script...")
    run_script(DEDUPLICATOR_SCRIPT, [RAW_CSV_PATH])
    print("✅ Step 2 completed successfully.\n")

    print("🔹 Step 3: Running Parse and Format Script...")
    run_script(PARSER_SCRIPT, [RAW_CSV_PATH])
    print("✅ Step 3 completed successfully.\n")

    print("\n✅ Full pipeline complete. Cleaned leads added to leads_registry.csv.")

    print("\n🔹 Step 4: Final deduplication of leads_registry.csv...")
    registry_path = os.path.join(ROOT_DIR, "data", "leads", "leads_registry.csv")
    run_script(DEDUPLICATOR_SCRIPT, [registry_path])
    print("✅ Step 4 completed successfully.\n")

if __name__ == "__main__":
    main()