import requests
import pandas as pd
from datetime import datetime, timedelta
from ambee_disater import get_disaster_cord
import os 
import json
import glob

def get_weather_data(latitude, longitude, disaster_timestamp):
    """
    Fetch hourly weather data from Open-Meteo Archive API
    and return the record closest to the disaster timestamp.
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
 
    #"https://archive-api.open-meteo.com/v1/archive"

    # Parse string timestamp if needed
    if isinstance(disaster_timestamp, str):
        disaster_timestamp = datetime.strptime(disaster_timestamp, "%Y-%m-%d %H:%M:%S")
    
    start_date = disaster_timestamp.strftime("%Y-%m-%d")
    end_date = (disaster_timestamp + timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "showers",
            "snowfall",
            "weather_code",
            "cloud_cover",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m"
        ],
        "timezone": "auto"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Create DataFrame for hourly data
        hourly_df = pd.DataFrame(data['hourly'])
        hourly_df['time'] = pd.to_datetime(hourly_df['time'])

        # Find row with closest timestamp to disaster
        closest_idx = (hourly_df['time'] - disaster_timestamp).abs().idxmin()
        closest_weather = hourly_df.loc[closest_idx]

        return closest_weather.to_dict()

    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None
    except KeyError:
        print("Hourly data not found in response.")
        return None

if __name__ == "__main__":
    print("🔍 Looking for disaster data files...")
    
    # Get the project root directory (two levels up from current file)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    # Search for JSON files in multiple locations
    search_paths = [
        os.path.dirname(__file__),  # Current directory
        project_root,               # Project root
        os.path.join(project_root, 'data')  # Data subdirectory if it exists
    ]
    
    disaster_files = []
    for path in search_paths:
        print(f"📁 Searching in: {path}")
        files = glob.glob(os.path.join(path, 'disaster_data_*.json'))
        if files:
            disaster_files.extend(files)
    
    if not disaster_files:
        print("❌ Error: No disaster_data_*.json files found in search paths.")
        print("💡 Please run the Ambee API script first to generate disaster data.")
        exit(1)
    
    # Use the most recent file
    filename = max(disaster_files, key=os.path.getctime)
    print(f"✅ Using disaster data from: {filename}")

    # Continue with the rest of your script...
    for i in range(10):
        try:
            disaster = get_disaster_cord(filename, i)
            if not disaster:
                print(f"⚠️ No data for disaster {i}")
                continue
                
            lat, lng, date_str = disaster['lat'], disaster['lng'], disaster['date']
            print(f"\n📍 Disaster {i}: {date_str}")
            weather = get_weather_data(lat, lng, date_str)
            if weather:
                print("✅ Closest Weather Observation:")
                serializable_weather = {k: (v.isoformat() if isinstance(v, pd.Timestamp) else v) 
                                     for k, v in weather.items()}
                print(json.dumps(serializable_weather, indent=2))
        except Exception as e:
            print(f"⚠️ Skipping disaster {i} due to error: {e}")
