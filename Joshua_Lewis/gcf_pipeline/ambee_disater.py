import requests
import json
import os
from datetime import datetime

# Optional: try to load from .env for local development only
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # Ignore if dotenv isn't installed in the cloud

# Now grab the key from environment variables
AMBEE_API_KEY = os.getenv("AMBEE_API_KEY")

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
    params = {"countryCode": country_code}

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return None
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Network error: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {str(e)}")
        return None

def get_disaster_cord(filename, index):
    with open(filename, 'r') as f:
        disaster = json.load(f)

    if not disaster or 'result' not in disaster:
        print("No disaster data available")
        return None

    coordinates = disaster['result'][index]
    return {
        'lat': coordinates.get('lat', None),
        'lng': coordinates.get('lng', None),
        'date': coordinates.get('date', None)
    }

def display_disaster_info(data):
    if not data or 'message' not in data:
        print("No data available")
        return

    print("\n=== Natural Disasters Report ===\n")
    for disaster in data.get('data', []):
        print(f"Type: {disaster.get('type', 'N/A')}")
        print(f"Title: {disaster.get('title', 'N/A')}")
        print(f"Description: {disaster.get('description', 'N/A')}")
        print(f"Severity: {disaster.get('severity', 'N/A')}")

# 🚫 REMOVE duplicate and test logic for deployment
# ✅ Keep this code clean for GCP, and use a separate script for local testing
