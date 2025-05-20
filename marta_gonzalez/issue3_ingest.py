import requests

def fetch_weather_forecast(lat=38.9072, lon=-77.0369):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "America/New_York"
    }
    return requests.get(url, params=params).json()

def fetch_air_quality():
    url = "https://data.cityofnewyork.us/resource/c3uy-2p5r.json"
    return requests.get(url).json()

def fetch_usgs_water(site="01646500"):
    url = "https://waterservices.usgs.gov/nwis/iv/"
    params = {
        "sites": site,
        "parameterCd": "00060",
        "format": "json"
    }
    return requests.get(url, params=params).json()