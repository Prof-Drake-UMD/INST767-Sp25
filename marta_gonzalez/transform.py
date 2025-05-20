import base64
import json
import pandas as pd
from datetime import datetime

def transform_weather(raw):
    hourly = raw["hourly"]
    df = pd.DataFrame({
        "timestamp": hourly["time"],
        "temperature_c": hourly["temperature_2m"],
        "precipitation_mm": hourly["precipitation"],
        "wind_speed_mps": hourly["wind_speed_10m"]
    })
    df["latitude"] = raw["latitude"]
    df["longitude"] = raw["longitude"]
    df.to_csv("/tmp/weather_data.csv", index=False)
    print("✅ Weather data saved.")

def transform_air_quality(raw):
    hourly = raw.get("hourly", {})
    if not hourly:
        print("❌ No hourly data found in air quality response.")
        return

    df = pd.DataFrame(hourly)
    df["timestamp"] = pd.to_datetime(df["time"])
    df.drop(columns=["time"], inplace=True)
    cols = ["timestamp"] + [col for col in df.columns if col != "timestamp"]
    df = df[cols]
    df.to_csv("/tmp/air_quality.csv", index=False)
    print("✅ Air quality data saved.")

def transform_water(raw):
    ts_list = raw.get("value", {}).get("timeSeries", [])
    if not ts_list:
        print("⚠️ No water data returned.")
        return

    ts_data = ts_list[0]
    site_info = ts_data["sourceInfo"]
    values = ts_data["values"][0]["value"]

    df = pd.DataFrame(values)
    df["timestamp"] = pd.to_datetime(df["dateTime"])
    df["streamflow_cfs"] = df["value"].astype(float)
    df["site_id"] = site_info["siteCode"][0]["value"]
    df["site_name"] = site_info["siteName"]
    df["latitude"] = site_info["geoLocation"]["geogLocation"]["latitude"]
    df["longitude"] = site_info["geoLocation"]["geogLocation"]["longitude"]
    df = df[["timestamp", "site_id", "site_name", "streamflow_cfs", "latitude", "longitude"]]
    df.to_csv("/tmp/water_conditions.csv", index=False)
    print("✅ Water data saved.")

# --- Cloud Function Entry Point ---
def run_transform(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic."""
    try:
        if "data" not in event:
            raise ValueError("No data in Pub/Sub message.")

        message_data = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(message_data)

        print("🔁 Starting transformations...")
        transform_weather(data["weather"])
        transform_air_quality(data["air_quality"])
        transform_water(data["water"])
        print("✅ All transformations completed.")

    except Exception as e:
        print(f"❌ Error in transformation: {e}")
        raise
