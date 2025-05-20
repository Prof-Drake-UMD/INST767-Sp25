# main.py
from ingest import fetch_weather_forecast, fetch_air_quality, fetch_usgs_water
from transform import transform_weather, transform_air_quality, transform_water

def run_pipeline(request):
    print("📡 Fetching data...")
    weather = fetch_weather_forecast()
    air_quality = fetch_air_quality()
    water = fetch_usgs_water()

    print("🔄 Transforming and writing CSVs...")
    transform_weather(weather)
    transform_air_quality(air_quality)
    transform_water(water)

    return "✅ Pipeline executed successfully"
