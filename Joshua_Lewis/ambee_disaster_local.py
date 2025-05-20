import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path

# Get the directory containing the script
BASE_DIR = Path(__file__).resolve().parent

# Load environment variables from .env file
load_dotenv(BASE_DIR / ".env")

# Get API key and add debug print
AMBEE_API_KEY = os.getenv("AMBEE_API_KEY")
print(f"API Key found: {'Yes' if AMBEE_API_KEY else 'No'}")

def fetch_ambee_disasters(country_code='US'):

    if not AMBEE_API_KEY:
        print("ERROR: API key not found in environment variables")
        return None

    url = "https://api.ambeedata.com/disasters/latest/by-country-code"
    
    headers = {
        "x-api-key": AMBEE_API_KEY,
        "Content-type": "application/json",
        "Accept": "application/json"
    }
    
    params = {
        "countryCode": country_code
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            return None
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Network error: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {str(e)}")
        return None

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

def display_disaster_info(data):
    """
    Display formatted disaster information
    Args:
        data (dict): JSON response from Ambee API
    """
    if not data or 'message' not in data:
        print("No data available")
        return

    print("\n=== Natural Disasters Report ===\n")
    
    for disaster in data.get('data', []):
        print(f"Type: {disaster.get('type', 'N/A')}")
        print(f"Title: {disaster.get('title', 'N/A')}")
        print(f"Description: {disaster.get('description', 'N/A')}")
        print(f"Severity: {disaster.get('severity', 'N/A')}")
        
        coordinates = get_disaster_cord(disaster)
        if coordinates:
            print(f"Location: Lat {coordinates['lat']}, "
                  f"Lon {coordinates['lng']}, "
                  f"Date {coordinates['date']}")

if __name__ == "__main__":
    print(os.listdir())
    print("Fetching natural disaster data from Ambee...")
    results = fetch_ambee_disasters(country_code="USA")

    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"disaster_data_{timestamp}.json"

        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nData saved to {filename}")

        print("\nDisplaying formatted data:")
        display_disaster_info(results)

        print("\nRaw JSON data:")
        print(json.dumps(results, indent=2))
    else:
        print("Failed to fetch data from Ambee API")
