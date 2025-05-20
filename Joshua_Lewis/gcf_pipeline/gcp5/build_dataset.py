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

def build_combined_rows(disaster_file, limit=10):
    rows = []

    for i in range(limit):
        try:
            disaster = get_disaster_cord(disaster_file, i)
            lat, lon, timestamp = disaster['lat'], disaster['lng'], disaster['date']
            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")

            weather = get_weather_data(lat, lon, timestamp)
            if not weather:
                print(f"⚠️ No weather data for disaster {i}, skipping...")
                continue

            address = reverse_geocode(lat, lon)

            row = {
                "event_id": str(uuid.uuid4()),
                "type": "UNKNOWN",
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


def save_to_csv(rows, filename="/tmp/disaster_dataset.csv"):
    if not rows:
        print("❌ No data to save.")
        return

    keys = rows[0].keys()
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Dataset saved to {filename}")
