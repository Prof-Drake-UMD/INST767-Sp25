import requests
import pandas as pd
from datetime import datetime, timedelta

def get_weather_data(latitude, longitude, disaster_timestamp):
    """
    Fetch hourly weather data from Open-Meteo Archive API
    and return the record closest to the disaster timestamp.
    """
    base_url = "https://api.open-meteo.com/v1/forecast"

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
