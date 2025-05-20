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
