import requests
import os
import json
from datetime import datetime

# Create a folder to store raw data
RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)


def save_json(data, filename):
    """Save response JSON to a file"""
    path = os.path.join(RAW_DATA_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved: {filename}")


def fetch_dc_data():
    """Fetch DC gun violence data (GeoJSON format)"""
    print("Fetching DC data...")
    url = "https://opendata.arcgis.com/datasets/89bfd2aed9a142249225a638448a5276_29.geojson"
    response = requests.get(url)
    data = response.json()
    save_json(data, f"dc_data_{datetime.today().date()}.geojson")
    return data


def fetch_nyc_data(limit=1000):
    """Fetch NYC gun crime complaints from Socrata"""
    print("Fetching NYC data...")
    url = "https://data.cityofnewyork.us/resource/5uac-w243.json"
    params = {
        "$where": "pd_desc LIKE '%FIREARM%'",
        "$limit": limit
    }
    response = requests.get(url, params=params)
    data = response.json()
    save_json(data, f"nyc_data_{datetime.today().date()}.json")
    return data

def fetch_cdc_data(limit=1000):
    print("Fetching CDC data...")
    url = "https://data.cdc.gov/resource/fpsi-y8tj.json"
    params = {"$limit": limit, "$where": "intent LIKE 'FA_%'"}
    response = requests.get(url, params=params)
    data = response.json()
    save_json(data, f"cdc_state_data_{datetime.today().date()}.json")
    return data




if __name__ == "__main__":
    dc_data = fetch_dc_data()
    nyc_data = fetch_nyc_data()
    cdc_data = fetch_cdc_data()

    print("All data fetched and saved.")












