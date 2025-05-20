import requests
import json
from google.cloud import pubsub_v1

# --- Open-Meteo Weather API ---
def fetch_weather_forecast(lat=38.9072, lon=-77.0369):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "America/New_York"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# --- Open-Meteo Air Quality API ---
def fetch_air_quality():
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": 38.9072,
        "longitude": -77.0369,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide",
        "timezone": "America/New_York"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# --- USGS Water Services API ---
def fetch_usgs_water(site="01646500"):
    url = "https://waterservices.usgs.gov/nwis/iv/"
    params = {
        "sites": site,
        "parameterCd": "00060",
        "format": "json"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# --- Cloud Function Entry Point ---
def ingest_data(request):
    # Call your existing functions
    weather = fetch_weather_forecast()
    air_quality = fetch_air_quality()
    water = fetch_usgs_water()

    # Combine results into one message
    message = {
        "weather": weather,
        "air_quality": air_quality,
        "water": water
    }

    # Publish to Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("dc-env-project-460403", "data-ingested")

    future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    print(f"✅ Published message to Pub/Sub: {future.result()}")

    return "✅ Ingest function completed"
