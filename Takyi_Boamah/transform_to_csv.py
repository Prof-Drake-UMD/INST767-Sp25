import os
import json
import pandas as pd
from datetime import datetime

# Paths inside Cloud Composer
api_results_path = "/home/airflow/gcs/dags/api_results"
output_path = "/home/airflow/gcs/dags/data"

# For local use
# api_results_path = "./api_results"
# output_path = "./data"

os.makedirs(output_path, exist_ok=True)

def transform_carbon():
    file_path = os.path.join(api_results_path, "carbon_estimates.json")
    with open(file_path, 'r') as f:
        carbon_data = json.load(f)
    df = pd.DataFrame(carbon_data)
    output_file = os.path.join(output_path, "carbon_emissions.csv")
    df.to_csv(output_file, index=False)
    print(f"✅ Transformed Carbon data to {output_file}")

def transform_weather():
    file_path = os.path.join(api_results_path, "weather_data.json")
    with open(file_path, 'r') as f:
        weather_data = json.load(f)
    
    weather_records = []
    for entry in weather_data:
        weather = entry.get("weather", {})
        main = weather.get("main", {})
        wind = weather.get("wind", {})
        clouds = weather.get("clouds", {})
        record = {
            "state": entry.get("state"),
            "city": entry.get("city"),
            "datetime": weather.get("dt"),
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
        weather_records.append(record)

    df = pd.DataFrame(weather_records)
    output_file = os.path.join(output_path, "weather_snapshot.csv")
    df.to_csv(output_file, index=False)
    print(f"✅ Transformed Weather data to {output_file}")

def transform_eia():
    file_path = os.path.join(api_results_path, "state_electricity_profile.json")
    with open(file_path, 'r') as f:
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
    output_file = os.path.join(output_path, "state_electricity_emissions.csv")
    df.to_csv(output_file, index=False)
    print(f"✅ Transformed EIA data to {output_file}")


