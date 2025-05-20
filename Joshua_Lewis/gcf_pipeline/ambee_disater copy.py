import requests
import json
from datetime import datetime

# API Configuration - hardcoded 
API_KEY = "73998db8f546f313521a71bd5a0c8cf8117ffadc224f468757a38acafd9a8e90"

def fetch_ambee_disasters(country_code='US'):
    """
    Fetch disaster data from Ambee API
    """
    url = "https://api.ambeedata.com/disasters/latest/by-country-code"
    print(f"🌐 Making request to: {url}")
    
    headers = {
        "x-api-key": API_KEY,
        "Content-type": "application/json",
        "Accept": "application/json"
    }
    print(f"🔑 Using API key: {API_KEY[:8]}...")
    
    params = {
        "countryCode": country_code
    }
    print(f"📍 Country code: {country_code}")

    try:
        print("📡 Sending request...")
        response = requests.get(url, headers=headers, params=params)
        print(f"📥 Response status code: {response.status_code}")
        print(f"📄 Response headers: {dict(response.headers)}")
        
        if response.status_code != 200:
            print(f"❌ Error response: {response.text}")
            return None
        
        data = response.json()
        print(f"✅ Successfully parsed JSON response")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ JSON parsing error: {str(e)}")
        return None

def display_disaster_info(data):
    """Display formatted disaster information"""
    if not data:
        print("⚠️ No data to display")
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
        print("⚠️ No disaster data available")
        return None

    try:
        coordinates = disaster.get('result')[index]
        return {
            'lat': coordinates.get('lat', None),
            'lng': coordinates.get('lng', None),
            'date': coordinates.get('date', None)
        }
    except (IndexError, KeyError) as e:
        print(f"⚠️ Error getting coordinates for index {index}: {str(e)}")
        return None

if __name__ == "__main__":
    # Fetch data for US
    print("Fetching natural disaster data from Ambee...")
    results = fetch_ambee_disasters(country_code="USA")

    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"disaster_data_{timestamp}.json"

        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nData saved to {filename}")
        
        # Display formatted data
        print("\nDisplaying formatted data:")
        display_disaster_info(results)
        
        # Display raw JSON
        print("\nRaw JSON data:")
        print(json.dumps(results, indent=2))
    else:
        print("Failed to fetch data from Ambee API")

#Attempt 22:33