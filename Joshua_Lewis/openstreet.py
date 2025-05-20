import requests

# Define your User-Agent info
USER_AGENT = "HomeportApp/1.0 (jlewis28@umd.edu)"  # Replace with your real email

def geocode(address):
    """
    Convert a physical address to latitude and longitude using OSM Nominatim API.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "json",
        "limit": 1
    }
    headers = {
        "User-Agent": USER_AGENT
    }

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200 and response.json():
        data = response.json()[0]
        return float(data['lat']), float(data['lon'])
    else:
        print("Geocoding failed.")
        return None


def reverse_geocode(lat, lon):
    """
    Convert latitude and longitude to an address using OSM Nominatim API.
    """
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "lat": lat,
        "lon": lon,
        "format": "json"
    }
    headers = {
        "User-Agent": USER_AGENT
    }

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        return response.json().get("display_name", "Address not found")
    else:
        print("Reverse geocoding failed.")
        return None


# Example usage:
if __name__ == "__main__":
    address = "1600 Pennsylvania Ave NW, Washington, DC"
    coords = geocode(address)
    if coords:
        print(f"\nCoordinates for '{address}': {coords}")
        rev_address = reverse_geocode(*coords)
        print(f"\nReverse geocoded address: {rev_address}")

