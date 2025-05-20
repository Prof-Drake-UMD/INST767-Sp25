import functions_framework
import os
import json
import uuid
import requests
from datetime import datetime
from google.cloud import storage
from weather import get_weather_data
from open_street import reverse_geocode



# === 🔐 API KEYS ===
AMBEE_API_KEY = "73998db8f546f313521a71bd5a0c8cf8117ffadc224f468757a38acafd9a8e90"
BUCKET_NAME = "gcf-v2-uploads-964013335183.us-east1.cloudfunctions.appspot.com"

# === Fetch Data ===
def fetch_ambee_disasters(country_code='USA'):
    url = "https://api.ambeedata.com/disasters/latest/by-country-code"
    headers = {
        "x-api-key": AMBEE_API_KEY,
        "Content-type": "application/json",
        "Accept": "application/json"
    }
    params = { "countryCode": country_code }

    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"⚠️ Ambee error: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Fetch error: {e}")
        return None

# === Dataset Builder ===
def get_disaster_cord(disaster_json, index):
    if not disaster_json or 'result' not in disaster_json:
        return None
    try:
        item = disaster_json['result'][index]
        return {
            "lat": item.get("lat"),
            "lng": item.get("lng"),
            "date": item.get("date")
        }
    except IndexError:
        return None

def build_combined_rows(disaster_json, limit=10):
    rows = []
    for i in range(limit):
        try:
            disaster = get_disaster_cord(disaster_json, i)
            if not disaster:
                continue
            lat, lon, timestamp = disaster['lat'], disaster['lng'], disaster['date']
            weather = get_weather_data(lat, lon, timestamp)
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
            print(f"⚠️ Skipping index {i}: {e}")
    return rows

# === Save JSON to File ===
def save_to_json(rows, filename):
    with open(filename, 'w') as f:
        json.dump(rows, f, indent=2)

# === Upload to GCS ===
def upload_to_gcs(bucket_name, local_path, dest_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_filename(local_path)
    print(f"📤 Uploaded {local_path} to gs://{bucket_name}/{dest_blob_name}")

# === Cloud Function Entry Point ===
@functions_framework.http
def run_pipeline(request):
    try:
        print("🚀 Running Cloud Function pipeline...")

        disaster_json = fetch_ambee_disasters()
        if not disaster_json:
            return ("Failed to fetch disaster data", 500)

        rows = build_combined_rows(disaster_json, limit=10)
        if not rows:
            return ("No disaster data rows created", 500)

        # Save JSON locally
        tmp_path = "/tmp/disaster_dataset.json"
        save_to_json(rows, tmp_path)
        print(f"💾 Saved JSON to: {tmp_path}")

        # Upload to GCS
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_filename = f"disaster_dataset_{timestamp}.json"
        upload_to_gcs(BUCKET_NAME, tmp_path, gcs_filename)

        return ("✅ JSON pipeline executed and uploaded successfully", 200)
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return (f"Pipeline failed: {str(e)}", 500)
