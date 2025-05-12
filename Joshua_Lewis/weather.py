import requests
import pandas as pd
from datetime import datetime

def get_weather_data(latitude=52.52, longitude=13.41):
    """
    Fetch detailed weather data from Open-Meteo API
    Args:
        latitude (float): Location latitude
        longitude (float): Location longitude
    Returns:
        tuple: (current_conditions DataFrame, hourly_forecast DataFrame)
    """
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
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
    current_weather, hourly_forecast = get_weather_data()
    
    if current_weather is not None:
        print("\nCurrent Weather Conditions:")
        print(current_weather)
        
    if hourly_forecast is not None:
        print("\nHourly Forecast (first 5 entries):")
        print(hourly_forecast.head())