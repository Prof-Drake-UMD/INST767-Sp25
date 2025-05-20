import functions_framework
from build_dataset import get_latest_disaster_file, build_combined_rows, save_to_csv
# from google.cloud import storage  # Uncomment if needed for GCS uploads
# from datetime import datetime     # Uncomment if needed for timestamps

@functions_framework.http
def run_pipeline(request):
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
        
        # Save to /tmp for Cloud Functions
        output_file = save_to_csv(dataset_rows, filename="/tmp/disaster_dataset.csv")
        print(f"💾 Saved data to {output_file}")
        
        # # Optional: Upload to Google Cloud Storage
        # bucket_name = os.environ.get('GCS_BUCKET_NAME')
        # if bucket_name:
        #     client = storage.Client()
        #     bucket = client.bucket(bucket_name)
        #     timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        #     blob = bucket.blob(f"disaster_dataset_{timestamp}.csv")
        #     blob.upload_from_filename(output_file)
        #     print(f"📤 Uploaded to gs://{bucket_name}/{blob.name}")
        
        return "Pipeline executed successfully", 200
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return f"Pipeline failed: {str(e)}", 500

if __name__ == "__main__":
    print("🧪 Local Pipeline Test")
    print("=" * 50)
    
    # Create mock request
    class MockRequest:
        pass
    
    try:
        # Run pipeline with mock request
        response, status = run_pipeline(MockRequest())
        
        print("\n📊 Test Results:")
        print(f"Status Code: {status}")
        print(f"Response: {response}")
        
        # Check output file
        import os
        csv_path = "/tmp/disaster_dataset.csv"
        if os.path.exists(csv_path):
            print(f"\n📄 CSV Preview ({csv_path}):")
            with open(csv_path, 'r') as f:
                head = [next(f) for _ in range(5)]
                print(''.join(head))
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")