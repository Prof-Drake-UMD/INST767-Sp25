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
    df.to_csv("data/weather_data.csv", index=False)

def transform_air_quality(raw):
    """
    Transforms raw JSON from Open-Meteo Air Quality API
    into a flat table and writes CSV.
    """
    # Extract hourly data dictionary
    hourly = raw.get("hourly", {})
    if not hourly:
        print("❌ No hourly data found in air quality response.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(hourly)

    # Convert time strings to datetime objects
    df["timestamp"] = pd.to_datetime(df["time"])
    df.drop(columns=["time"], inplace=True)

    # Optional: reorder columns
    cols = ["timestamp"] + [col for col in df.columns if col != "timestamp"]
    df = df[cols]

    # Save to CSV
    df.to_csv("data/air_quality.csv", index=False)
    print(f"✅ Air quality data saved with {len(df)} records.")

def transform_water(raw):
    ts_list = raw.get("value", {}).get("timeSeries", [])
    if not ts_list:
        print("⚠️ No water data returned. Check the site ID or parameters.")
        return  # Exit gracefully

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
    df.to_csv("data/water_conditions.csv", index=False)