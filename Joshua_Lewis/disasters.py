import requests
import json

def fetch_disaster_reports(limit=2):
    """
    Fetch disaster reports from ReliefWeb API
    Args:
        limit (int): Number of reports to retrieve (default: 2)
    Returns:
        dict: JSON response from the API
    """
    url = "https://api.reliefweb.int/v1/reports"
    params = {
        "appname": "my_disaster_app",  # Replace with your app name
        "limit": limit
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
'''
if __name__ == "__main__":
    results = fetch_disaster_reports(limit=2)
    if results and 'data' in results:
        print("\n=== Sample Disaster Reports ===\n")
        
        for report in results['data']:
            fields = report['fields']
            print(f"Title: {fields.get('title', 'N/A')}")
            print(f"Date: {fields.get('date', {}).get('created', 'N/A')}")
            print(f"Country: {', '.join(c['name'] for c in fields.get('country', []))}")
            print(f"Source: {', '.join(s['name'] for s in fields.get('source', []))}")
            print(f"URL: {fields.get('url', 'N/A')}")
            print("-" * 80 + "\n")
'''

if __name__ == "__main__":
    results = fetch_disaster_reports(limit=2)
    if results and 'data' in results:
        print("\n=== Sample Disaster Reports ===\n")
        
        for report in results['data']:
            fields = report['fields']
            print(f"Title: {fields.get('title', 'N/A')}")
            print(f"Date: {fields.get('date', {}).get('created', 'N/A')}")
            print(f"Country: {', '.join(c['name'] for c in fields.get('country', []))}")
            print(f"Source: {', '.join(s['name'] for s in fields.get('source', []))}")
            print(f"URL: {fields.get('url', 'N/A')}")

            # Extract location data if available
            locations = fields.get('location', [])
            if locations:
                for loc in locations:
                    print(f"Location: {loc.get('name', 'N/A')}")
                    print(f"Latitude: {loc.get('lat', 'N/A')}, Longitude: {loc.get('lon', 'N/A')}")
            else:
                print("Location: N/A")

            print("-" * 80 + "\n")
