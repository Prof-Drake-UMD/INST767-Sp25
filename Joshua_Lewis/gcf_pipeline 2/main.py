
from build_dataset import get_latest_disaster_file, build_combined_rows, save_to_csv

def run_pipeline(request):
    try:
        file = get_latest_disaster_file()
        rows = build_combined_rows(file, limit=3)  # test with 3 to stay within limits
        save_to_csv(rows)
        return "✅ Pipeline completed and CSV saved."
    except Exception as e:
        return f"❌ Pipeline failed: {e}"
