from ambee_disater import fetch_ambee_disasters
from ingest import get_weather_data
from osm_data import get_nearby_streets
from osm_data import geocode, reverse_geocode
import pandas as pd

def get_combined_data():
    """Combine data from all three APIs for each disaster location"""
    
    # Get disaster data
    disasters = fetch_ambee_disasters()
    
    combined_data = []
    
    if disasters and 'data' in disasters:
        for disaster in disasters['data']:
            location = disaster.get('coordinates', {})
            lat = location.get('lat')
            lon = location.get('lng')
            
            if lat and lon:
                # Get weather data
                current_weather, hourly_forecast = get_weather_data(lat, lon)
                
                # Get nearby streets
                streets = get_nearby_streets(lat, lon)
                
                combined_data.append({
                    'disaster': disaster,
                    'weather': current_weather,
                    'forecast': hourly_forecast,
                    'streets': streets
                })
    
    return combined_data

if __name__ == "__main__":
    results = get_combined_data()
    
    for location in results:
        print("\n" + "="*80)
        print(f"Disaster: {location['disaster'].get('title', 'N/A')}")
        print(f"Location: {location['disaster']['coordinates'].get('lat')}, "
              f"{location['disaster']['coordinates'].get('lng')}")
        
        print("\nCurrent Weather:")
        print(location['weather'])
        
        print("\nNearby Streets:")
        print(location['streets'].head() if location['streets'] is not None else "No street data available")
        print("="*80)