from ingest import fetch_air_quality, fetch_weather_forecast, fetch_usgs_water
from transform import transform_air_quality, transform_weather, transform_water

def main():
    print("📡 Fetching data...")
    air_quality_raw = fetch_air_quality()
    weather_raw = fetch_weather_forecast()
    water_raw = fetch_usgs_water()

    print("🔄 Transforming and writing CSVs...")
    transform_air_quality(air_quality_raw)
    transform_weather(weather_raw)
    transform_water(water_raw)

    print("✅ Pipeline complete. CSVs are in /data")

if __name__ == "__main__":
    main()