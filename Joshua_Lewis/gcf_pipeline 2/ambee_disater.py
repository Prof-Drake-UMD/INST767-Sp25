import json
import requests
from datetime import datetime
from google.cloud import storage

# API Configuration - hardcoded for GCP
API_KEY = "73998db8f546f313521a71bd5a0c8cf8117ffadc224f468757a38acafd9a8e90"
BUCKET_NAME = "gcf-v2-uploads-964013335183.us-east1.cloudfunctions.appspot.com"  # Replace with your actual bucket name

def fetch_ambee_disasters(country_code='USA'):
    """
    Fetch disaster data from Ambee API
    """
    url = "https://api.ambeedata.com/disasters/latest/by-country-code"
    
    headers = {
        "x-api-key": API_KEY,
        "Content-type": "application/json",
        "Accept": "application/json"
    }
    
    params = {
        "countryCode": country_code
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"API Response Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Error response: {response.text}")
            return None
            
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Network error: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {str(e)}")
        return None

def display_disaster_info(data):
    """
    Display formatted disaster information
    """
    if not data or 'data' not in data:
        print("⚠️ No valid data received")
        return

    print("\n=== Natural Disasters Report ===\n")
    
    for disaster in data.get('data', []):
        print(f"Type: {disaster.get('type', 'N/A')}")
        print(f"Title: {disaster.get('title', 'N/A')}")
        print(f"Description: {disaster.get('description', 'N/A')}")
        print(f"Severity: {disaster.get('severity', 'N/A')}")
        print("-" * 30)

def get_disaster_cord(filename, index):
    """
    Get coordinates of the disaster
    Args:
        filename (str): Path to the saved JSON file
        index (int): Index of the disaster in the result list
    Returns:
        dict: Coordinates and date of the disaster
    """
    with open(filename, 'r') as f: 
        disaster = json.load(f)

    if not disaster or 'result' not in disaster:
        print("No disaster data available")
        return None

    coordinates = disaster.get('result')[index]
    return {
        'lat': coordinates.get('lat', None),
        'lng': coordinates.get('lng', None),
        'date': coordinates.get('date', None)
    }


def upload_to_gcs(bucket_name, data, filename):
    """
    Upload data to Google Cloud Storage
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    
    blob.upload_from_string(
        data=json.dumps(data),
        content_type='application/json'
    )
    print(f"Data uploaded to {filename} in bucket {bucket_name}")

def run_pipeline_clean(request):
    """
    Cloud Function entry point
    """
    print("Starting disaster data pipeline...")
    
    # Fetch disaster data
    results = fetch_ambee_disasters()
    
    if not results:
        return ('Failed to fetch disaster data', 400)
    
    try:
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"disaster_data_{timestamp}.json"
        
        # Upload to Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        
        blob.upload_from_string(
            data=json.dumps(results, indent=2),
            content_type='application/json'
        )
        
        print(f"Data saved to gs://{BUCKET_NAME}/{filename}")
        return ('Pipeline completed successfully', 200)
        
    except Exception as e:
        print(f"Error in pipeline: {str(e)}")
        return (f'Pipeline error: {str(e)}', 500)

# For local testing
if __name__ == "__main__":
    print("Testing disaster data fetch...")
    results = fetch_ambee_disasters()
    if results:
        print(json.dumps(results, indent=2))
