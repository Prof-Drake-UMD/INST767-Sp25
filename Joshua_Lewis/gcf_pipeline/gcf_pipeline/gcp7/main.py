import functions_framework
import os
from datetime import datetime
from google.cloud import storage
from build_dataset import get_latest_disaster_file, build_combined_rows, save_to_csv

@functions_framework.http
def run_pipeline(request):
    """
    Cloud Function Gen1 entry point that processes disaster data and uploads to GCS.
    Args:
        request: Flask request object
    Returns:
        tuple: (response message, HTTP status code)
    """
    try:
        print("🚀 Starting pipeline...")
        
        file = get_latest_disaster_file()
        print(f"📂 Using disaster file: {file}")
        
        dataset_rows = build_combined_rows(file)
        print(f"✅ Generated {len(dataset_rows)} rows of data")
        
        # Save to /tmp directory (writable in Gen1)
        tmp_output = "/tmp/disaster_dataset.csv"
        save_to_csv(dataset_rows, filename=tmp_output)
        print(f"💾 Saved data to temporary file: {tmp_output}")
        
        # Upload to GCS bucket
        bucket_name = os.environ.get('GCS_BUCKET_NAME')
        if not bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable not set")
            
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"disaster_data_{timestamp}.csv"
        blob = bucket.blob(blob_name)
        
        blob.upload_from_filename(tmp_output)
        print(f"📤 Uploaded to gs://{bucket_name}/{blob_name}")
        
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