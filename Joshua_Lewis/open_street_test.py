import requests
import os
import glob
from ambee_disater import get_disaster_cord  # should return dict with lat, lng, timestamp

# Define your User-Agent info
USER_AGENT = "HomeportApp/1.0 (jlewis28@umd.edu)"

def reverse_geocode(lat, lon):
    """
    Convert latitude and longitude to an address using OSM Nominatim API.
    """
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json"}
    headers = {"User-Agent": USER_AGENT}

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        return response.json().get("display_name", "Address not found")
    else:
        print("Reverse geocoding failed.")
        return None
'''
if __name__ == "__main__":
    # Find the most recent Ambee disaster JSON file
    disaster_files = sorted(glob.glob(os.path.join(os.path.dirname(__file__), 'disaster_data_*.json')), reverse=True)
    if not disaster_files:
        print("❌ No disaster data files found.")
        exit()

    filename = disaster_files[0]
    print(f"📂 Using disaster data from: {filename}")

    # Loop through disasters and reverse geocode each one
    for i in range(10):
        try:
            lat, lon, timestamp = get_disaster_cord(filename, i).values()
            print(f"\n📍 Disaster {i}: {timestamp} at ({lat}, {lon})")
            address = reverse_geocode(lat, lon)
            print(f"🏠 Reverse geocoded address: {address}")
        except Exception as e:
            print(f"⚠️ Error with disaster {i}: {e}")
'''