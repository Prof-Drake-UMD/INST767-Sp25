import os
import json
import pandas as pd
from datetime import datetime

data_dir = "./api_results"
output_dir = "./transformed"
os.makedirs(output_dir, exist_ok=True)

def transform_carbon():
    path = os.path.join(data_dir, "carbon_estimates_2025-04-09_20-41-18.json")
    with open(path) as f:
        carbon_data = json.load(f)
    df = pd.DataFrame(carbon_data)
    df["estimated_at"] = datetime.now().isoformat()
    df.to_csv(os.path.join(output_dir, "carbon_emissions.csv"), index=False)
    print("✅ carbon_emissions.csv saved.")

def transform_weather():
    path = os.path.join(data_dir, "weather_data_2025-04-09_20-23-52.json")
    with open(path) as f:
        weather_data = json.load(f)

    records = []
    for entry in weather_data:
        weather = entry.get("weather", {})
        main = weather.get("main", {})
        wind = weather.get("wind", {})
        clouds = weather.get("clouds", {})
        record = {
            "state": entry.get("state"),
            "city": entry.get("city"),
            "datetime": datetime.utcfromtimestamp(weather.get("dt", 0)).isoformat(),
            "temp_celsius": main.get("temp"),
            "feels_like_celsius": main.get("feels_like"),
            "humidity_percent": main.get("humidity"),
            "pressure_hpa": main.get("pressure"),
            "weather_main": weather.get("weather", [{}])[0].get("main"),
            "weather_description": weather.get("weather", [{}])[0].get("description"),
            "wind_speed_mps": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "cloud_percent": clouds.get("all"),
            "visibility_m": weather.get("visibility")
        }
        records.append(record)

    pd.DataFrame(records).to_csv(os.path.join(output_dir, "weather_snapshot.csv"), index=False)
    print("✅ weather_snapshot.csv saved.")

def transform_eia():
    path = os.path.join(data_dir, "state_electricity_profile_2025-04-09_20-13-52.json")
    with open(path) as f:
        eia_data = json.load(f)
    df = pd.DataFrame(eia_data)
    df = df.rename(columns={
        "period": "year",
        "stateid": "state",
        "fuelid": "fuel_type",
        "fuelDescription": "fuel_description",
        "co2-thousand-metric-tons": "co2_thousand_mt"
    })
    df["year"] = df["year"].astype(int)
    df["co2_thousand_mt"] = pd.to_numeric(df["co2_thousand_mt"], errors="coerce")
    df.to_csv(os.path.join(output_dir, "state_electricity_emissions.csv"), index=False)
    print("✅ state_electricity_emissions.csv saved.")
