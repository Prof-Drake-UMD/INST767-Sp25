import requests
import json
from datetime import datetime, timedelta

def fetch_gdacs_disasters():
    """
    Fetch natural disasters from GDACS API for the United States
    Returns:
        dict: JSON response from the API
    """
    # GDACS API endpoint for events
    url = "https://www.gdacs.org/gdacsapi/swagger/v1/swagger.json"
    
    try:
        # Make the GET request
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Filter for US events
        us_events = [event for event in data 
                    if isinstance(event.get('countrydata'),list) and any('United States' in country.get('name', '') 
                           for country in event['countrydata'])]
        
        return us_events
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    print("Fetching US natural disaster data from GDACS...")
    disasters = fetch_gdacs_disasters()
    
    if disasters:
        print("\n=== Natural Disasters in the United States ===\n")
        for event in disasters:
            print(f"Event Type: {event.get('eventtype', 'N/A')}")
            print(f"Alert Level: {event.get('alertlevel', 'N/A')}")
            print(f"Event Name: {event.get('eventname', 'N/A')}")
            print(f"Event ID: {event.get('eventid', 'N/A')}")
            print(f"Date: {event.get('fromdate', 'N/A')}")
            
            if 'point' in event:
                print(f"Location: Lat {event['point'].get('lat', 'N/A')}, Lon {event['point'].get('lon', 'N/A')}")
            
            print("-" * 80 + "\n")