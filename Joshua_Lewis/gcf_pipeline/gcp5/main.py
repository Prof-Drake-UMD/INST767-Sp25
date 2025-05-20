from build_dataset import build_combined_rows, save_to_csv
from ambee_disater import fetch_ambee_disasters
import json

def run_pipeline(request):
    try:
        # Step 1: Fetch disaster data from Ambee
        data = fetch_ambee_disasters()
        if not data or 'result' not in data:
            return "❌ No disaster data returned from API."

        # Step 2: Save disaster data to a temp file
        filename = "/tmp/disaster_data_live.json"
        with open(filename, "w") as f:
            json.dump(data, f)

        # Step 3: Build dataset and save to CSV
        rows = build_combined_rows(filename, limit=3)
        save_to_csv(rows)

        return "✅ Pipeline completed and CSV saved."

    except Exception as e:
        return f"❌ Pipeline failed: {e}"
