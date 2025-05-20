import os
import glob
import csv
import uuid
import json
import pandas as pd
from datetime import datetime
from ambee_disater import get_disaster_cord
from testweather import get_weather_data
from open_street_test import reverse_geocode

# Load most recent disaster JSON file
def get_latest_disaster_file():
    files = sorted(glob.glob("disaster_data_*.json"), reverse=True)
    if not files:
        raise FileNotFoundError("❌ No disaster_data_*.json files found.")
    return files[0]

# Create final dataset row per disaster
def build_combined_rows(disaster_file, limit=10):
    rows = []

    for i in range(limit):
        try:
            # Step 1: Get lat/lon/timestamp
            disaster = get_disaster_cord(disaster_file, i)
            lat, lon, timestamp = disaster['lat'], disaster['lng'], disaster['date']
            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

            # Step 2: Get weather
            weather = get_weather_data(lat, lon, timestamp)
            if not weather:
                print(f"⚠️ No weather data for disaster {i}, skipping...")
                continue

            # Step 3: Get address
            address = reverse_geocode(lat, lon)

            # Step 4: Combine
            row = {
                "event_id": str(uuid.uuid4()),
                "type": "UNKNOWN",  # Optional: replace if you can pull type from JSON
                "title": f"Disaster {i}",
                "timestamp": timestamp,
                "lat": lat,
                "lon": lon,
                "address": address,
                "temperature_2m": weather.get("temperature_2m"),
                "apparent_temperature": weather.get("apparent_temperature"),
                "precipitation": weather.get("precipitation"),
                "wind_speed_10m": weather.get("wind_speed_10m"),
                "wind_gusts_10m": weather.get("wind_gusts_10m"),
                "cloud_cover": weather.get("cloud_cover"),
                "weather_code": weather.get("weather_code")
            }

            rows.append(row)

        except Exception as e:
            print(f"⚠️ Skipping disaster {i} due to error: {e}")

    return rows

# Save rows to CSV
def save_to_csv(rows, filename="disaster_dataset.csv"):
    if not rows:
        print("❌ No data to save.")
        return

    keys = rows[0].keys()
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ Dataset saved to {filename}")

if __name__ == "__main__":
    try:
        file = get_latest_disaster_file()
        print(f"📂 Using disaster file: {file}")
        dataset_rows = build_combined_rows(file)
        save_to_csv(dataset_rows)
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
