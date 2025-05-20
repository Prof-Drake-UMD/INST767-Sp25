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
    df = pd.DataFrame(raw)
    # Select relevant columns
    df = df[[
        "start_date",
        "geo_place_name",
        "name",
        "measure",
        "measure_info",
        "data_value"
    ]]
    # Rename columns for clarity
    df.rename(columns={
        "start_date": "date",
        "geo_place_name": "location",
        "name": "pollutant",
        "measure": "unit",
        "measure_info": "unit_description",
        "data_value": "value"
    }, inplace=True)
    # Convert date to datetime format
    df["date"] = pd.to_datetime(df["date"])
    # Save to CSV
    df.to_csv("data/air_quality_data.csv", index=False)

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