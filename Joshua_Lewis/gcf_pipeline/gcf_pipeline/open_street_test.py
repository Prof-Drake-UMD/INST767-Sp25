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

if __name__ == "__main__":
    print("🔍 Looking for disaster data files...")
    
    # Search in multiple locations
    search_paths = [
        os.path.dirname(__file__),  # Current directory
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),  # Project root
        os.path.join(os.path.dirname(__file__), 'data')  # Data subdirectory
    ]
    
    disaster_files = []
    for path in search_paths:
        print(f"📁 Searching in: {path}")
        files = glob.glob(os.path.join(path, 'disaster_data_*.json'))
        if files:
            disaster_files.extend(files)
    
    if not disaster_files:
        print("❌ No disaster data files found in search paths.")
        print("💡 Please run ambee_disater.py first to generate disaster data.")
        exit(1)

    # Use most recent file
    filename = max(disaster_files, key=os.path.getctime)
    print(f"✅ Using disaster data from: {filename}")

    # Loop through disasters and reverse geocode each one
    for i in range(10):
        try:
            disaster = get_disaster_cord(filename, i)
            if not disaster:
                print(f"⚠️ No data for disaster {i}")
                continue
                
            lat, lon = disaster['lat'], disaster['lng']
            timestamp = disaster['date']
            
            print(f"\n📍 Disaster {i}: {timestamp}")
            print(f"   Coordinates: ({lat}, {lon})")
            
            # Add delay to respect Nominatim's usage policy
            if i > 0:
                time.sleep(1)  # Add 1 second delay between requests
                
            address = reverse_geocode(lat, lon)
            if address:
                print(f"🏠 Address: {address}")
            else:
                print("❌ Could not get address")
                
        except Exception as e:
            print(f"⚠️ Error processing disaster {i}: {str(e)}")
