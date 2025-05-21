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
    return df.to_dict(orient="records")  # 🔥 Add this line

def transform_air_quality(raw):
    hourly = raw.get("hourly", {})
    if not hourly:
        print("❌ No hourly data found in air quality response.")
        return []

    if "time" not in hourly:
        print("❌ 'time' key missing in air quality hourly data.")
        return []

    df = pd.DataFrame(hourly)
    df["timestamp"] = pd.to_datetime(df["time"]).dt.to_pydatetime()
    df.drop(columns=["time"], inplace=True)
    cols = ["timestamp"] + [col for col in df.columns if col != "timestamp"]
    df = df[cols]
    df.to_csv("/tmp/air_quality.csv", index=False)
    print("✅ Air quality data saved.")
    return df.to_dict(orient="records")


def transform_water(raw):
    ts_list = raw.get("value", {}).get("timeSeries", [])
    if not ts_list:
        print("⚠️ No water data returned.")
        return []

    ts_data = ts_list[0]
    site_info = ts_data["sourceInfo"]
    values = ts_data["values"][0]["value"]

    df = pd.DataFrame(values)
    df["timestamp"] = pd.to_datetime(df["dateTime"]).dt.to_pydatetime()
    df["streamflow_cfs"] = df["value"].astype(float)
    df["site_id"] = site_info["siteCode"][0]["value"]
    df["site_name"] = site_info["siteName"]
    df["latitude"] = site_info["geoLocation"]["geogLocation"]["latitude"]
    df["longitude"] = site_info["geoLocation"]["geogLocation"]["longitude"]
    df.drop(columns=["dateTime", "value"], inplace=True)
    df = df[["timestamp", "site_id", "site_name", "streamflow_cfs", "latitude", "longitude"]]
    df.to_csv("/tmp/water_conditions.csv", index=False)
    print("✅ Water data saved.")
    return df.to_dict(orient="records")

