import requests
import pandas as pd
from datetime import datetime
from ambee_disater import get_disaster_cord
import os 
import json
import glob

def get_weather_data(latitude, longitude, date):
    """
    Fetch detailed weather data from Open-Meteo API
    Args:
        latitude (float): Location latitude
        longitude (float): Location longitude
    Returns:
        tuple: (current_conditions DataFrame, hourly_forecast DataFrame)
    """
    base_url = "https://archive-api.open-meteo.com/v1/archive"

    date= date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S").date()

    start_date = date.strftime("%Y-%m-%d")
    end_date = date.strftime("%Y-%m-%d") + datetime.timedelta(days=1)
    
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
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "is_day",
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

        # Create DataFrame for current conditions
        current_df = pd.DataFrame([data['current']])
        current_df['timestamp'] = pd.to_datetime(data['current']['time'])

        # Create DataFrame for hourly forecast
        hourly_df = pd.DataFrame(data['hourly'])
        hourly_df['time'] = pd.to_datetime(hourly_df['time'])

        return current_df, hourly_df

    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None, None

if __name__ == "__main__":
    # Replace '*.json' with the actual filename or handle the wildcard properly
    filename = os.path.join(os.path.dirname(__file__), 'disaster_data.json')
    # Handle wildcard to find the correct file
    disaster_files = glob.glob(os.path.join(os.path.dirname(__file__), '*.json'))
    if disaster_files:
        filename = disaster_files[0]  # Use the first matching file
    else:
        raise FileNotFoundError("No JSON files found in the directory.")
   

    print(f"Using disaster data from: {filename}")

    for i in range(10): 
        lat,lng, date = get_disaster_cord(filename, i).values()
        print(date)
        current_weather, hourly_forecast = get_weather_data(lat, lng, date)
    
        if current_weather is not None:
            print("\nCurrent Weather Conditions:")
            print(current_weather)
        # print(current_weather.columns)
        
        if hourly_forecast is not None:
            print("\nHourly Forecast (first 5 entries):")
            print(hourly_forecast.head())
            #print(hourly_forecast.columns)
        