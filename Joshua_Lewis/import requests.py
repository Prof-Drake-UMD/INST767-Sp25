import requests
import pandas as pd

def get_nearby_streets(latitude, longitude, radius=1000):
    """
    Get streets near a location using OpenStreetMap's Overpass API
    Args:
        latitude (float): Location latitude
        longitude (float): Location longitude
        radius (int): Search radius in meters
    Returns:
        DataFrame: Nearby streets information
    """
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Query for streets within radius
    query = f"""
    [out:json];
    way["highway"]
      (around:{radius},{latitude},{longitude});
    out body;
    >;
    out skel qt;
    """
    
    try:
        response = requests.post(overpass_url, data={"data": query})
        response.raise_for_status()
        data = response.json()
        
        streets = []
        for element in data['elements']:
            if element['type'] == 'way':
                streets.append({
                    'id': element['id'],
                    'name': element.get('tags', {}).get('name', 'Unknown'),
                    'type': element.get('tags', {}).get('highway', 'Unknown'),
                    'distance': radius  # Actual distance calculation could be added
                })
                
        return pd.DataFrame(streets)
        
    except requests.RequestException as e:
        print(f"Error fetching street data: {e}")
        return None