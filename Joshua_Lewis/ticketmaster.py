import requests
import json
from datetime import datetime

def fetch_ticketmaster_events(api_key, keyword=None, city=None, limit=10):
    """
    Fetch events from Ticketmaster API
    Args:
        api_key (str): Your Ticketmaster API key
        keyword (str): Search term for events (optional)
        city (str): City name to filter events (optional)
        limit (int): Number of events to return (default: 10)
    Returns:
        dict: JSON response from the API
    """
    base_url = "https://app.ticketmaster.com/discovery/v2/events.json"
    
    # Build parameters
    params = {
        "apikey": api_key,
        "countryCode": "US",
        "size": limit
    }
    
    if keyword:
        params["keyword"] = keyword
    if city:
        params["city"] = city

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    # Replace with your actual API key
    API_KEY = "YOUR_API_KEY_HERE"
    
    print("Fetching events from Ticketmaster...")
    results = fetch_ticketmaster_events(
        api_key=API_KEY,
        keyword="concert",  # Optional: filter by keyword
        city="New York",    # Optional: filter by city
        limit=5
    )
    
    if results and "_embedded" in results:
        events = results["_embedded"]["events"]
        print("\n=== Upcoming Events ===\n")
        
        for event in events:
            print(f"Name: {event.get('name', 'N/A')}")
            
            # Date and time
            dates = event.get('dates', {}).get('start', {})
            date_str = dates.get('localDate', 'N/A')
            time_str = dates.get('localTime', 'N/A')
            print(f"Date: {date_str} at {time_str}")
            
            # Venue information
            if "_embedded" in event and "venues" in event["_embedded"]:
                venue = event["_embedded"]["venues"][0]
                venue_name = venue.get('name', 'N/A')
                city = venue.get('city', {}).get('name', 'N/A')
                state = venue.get('state', {}).get('stateCode', 'N/A')
                print(f"Venue: {venue_name} in {city}, {state}")
            
            # Price range
            if "priceRanges" in event:
                price_range = event["priceRanges"][0]
                min_price = price_range.get('min', 'N/A')
                max_price = price_range.get('max', 'N/A')
                currency = price_range.get('currency', 'USD')
                print(f"Price Range: {currency} {min_price} - {currency} {max_price}")
            
            # Ticket status
            status = event.get('dates', {}).get('status', {}).get('code', 'N/A')
            print(f"Status: {status}")
            
            # Event URL
            url = event.get('url', 'N/A')
            print(f"Tickets: {url}")
            
            print("-" * 80 + "\n")