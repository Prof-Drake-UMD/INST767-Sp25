import requests

USER_AGENT = "HomeportApp/1.0 (jlewis28@umd.edu)"  # Replace with your real email

def geocode(address):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json", "limit": 1}
    headers = {"User-Agent": USER_AGENT}

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200 and response.json():
        data = response.json()[0]
        return float(data['lat']), float(data['lon'])
    else:
        print("Geocoding failed.")
        return None

def reverse_geocode(lat, lon):
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {"lat": lat, "lon": lon, "format": "json"}
    headers = {"User-Agent": USER_AGENT}

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        return response.json().get("display_name", "Address not found")
    else:
        print("Reverse geocoding failed.")
        return None
