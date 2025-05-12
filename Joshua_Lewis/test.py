print("hello world")
print("hello world")
print("hello world")
print("hello world")
print("hello world")
print("hello world")
print("hello world")
print("hello world")    

import requests
from datetime import datetime

import requests
from datetime import datetime

def get_weather(city, api_key):
    """
    Fetch daily weather data for a given city using OpenWeatherMap API
    
    Args:
        city (str): City name
        api_key (str): OpenWeatherMap API key
    
    Returns:
        dict: Weather data if successful, None if request fails
    """
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"  # For Celsius
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        weather_data = response.json()
        
        result = {
            "city": weather_data["name"],
            "temperature": weather_data["main"]["temp"],
            "description": weather_data["weather"][0]["description"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return result
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None