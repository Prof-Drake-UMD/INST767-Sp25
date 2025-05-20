import functions_framework
from build_dataset import get_latest_disaster_file, build_combined_rows, save_to_csv

@functions_framework.http
def main(request):
    """
    Cloud Function entry point that processes disaster data and generates CSV.
    Args:
        request (flask.Request): The request object
    Returns:
        tuple: (response message, HTTP status code)
    """
    try:
        print("🚀 Starting pipeline...")
        
        file = get_latest_disaster_file()
        print(f"📂 Using disaster file: {file}")
        
        dataset_rows = build_combined_rows(file)
        print(f"✅ Generated {len(dataset_rows)} rows of data")
        
        output_file = save_to_csv(dataset_rows)
        print(f"💾 Saved data to {output_file}")
        
        return "Pipeline executed successfully", 200
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return f"Pipeline failed: {str(e)}", 500

#22:30