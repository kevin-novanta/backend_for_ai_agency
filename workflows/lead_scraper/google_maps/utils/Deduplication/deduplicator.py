import os
import pandas as pd

INPUT_PATH = os.path.join("data", "exports", "Google_Leads", "Cleaned_Google_Maps_Data", "enriched_data.csv")

def deduplicate_csv(input_path: str):
    print(f"🔄 Starting deduplication on: {input_path}")
    
    # Load the data
    try:
        df = pd.read_csv(input_path)
        print(f"📥 Loaded {len(df)} rows from CSV.")
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return

    # Deduplicate based on all columns
    df_deduped = df.drop_duplicates()
    print(f"🧹 Removed duplicates. {len(df_deduped)} rows remain after deduplication.")

    # Save back to the same file
    try:
        df_deduped.to_csv(input_path, index=False)
        print(f"✅ Overwrote original file with deduplicated data.")
    except Exception as e:
        print(f"❌ Failed to write CSV: {e}")
        return

if __name__ == "__main__":
    deduplicate_csv(INPUT_PATH)