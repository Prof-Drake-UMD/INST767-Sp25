# main.py
from ingest import fetch_weather_forecast, fetch_air_quality, fetch_usgs_water
from transform import transform_weather, transform_air_quality, transform_water
from google.cloud import pubsub_v1
import json
from google.cloud import pubsub_v1
from flask import Request
import base64


def run_pipeline(request):
    try:
        print("📡 Fetching data...")
        weather = fetch_weather_forecast()
        air_quality = fetch_air_quality()
        water = fetch_usgs_water()

        print("🔄 Transforming and writing CSVs...")
        transform_weather(weather)
        transform_air_quality(air_quality)
        transform_water(water)

        return ("✅ Pipeline executed successfully", 200)
    except Exception as e:
        print(f"❌ Error in pipeline: {e}")
        return (f"❌ Internal Server Error: {e}", 500)

# --- Cloud Function Entry Point ---
def ingest_data(request):
    # Call your existing functions
    weather = fetch_weather_forecast()
    air_quality = fetch_air_quality()
    water = fetch_usgs_water()

    # Combine results into one message
    message = {
        "weather": weather,
        "air_quality": air_quality,
        "water": water
    }

    # Publish to Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("dc-env-project-460403", "data-ingested")

    future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    print(f"✅ Published message to Pub/Sub: {future.result()}")

    return "✅ Ingest function completed"

def run_transform(event, context):
    print("📦 Raw event received:", event)
    """Triggered from a message on a Cloud Pub/Sub topic."""
    try:
        if "data" not in event:
            raise ValueError("No 'data' field in Pub/Sub message.")

        # Base64 decode
        raw_data = event["data"]
        if not raw_data:
            raise ValueError("Pub/Sub 'data' field is empty.")

        message_data = base64.b64decode(raw_data).decode("utf-8")
        print(f"🔍 Decoded message: {message_data}")

        # JSON parse
        if not message_data.strip():
            raise ValueError("Decoded message is empty or whitespace.")

        data = json.loads(message_data)
        print("📊 Parsed JSON object:", data)

        # Debug prints for each expected field
        print("🌤️ Weather data:", data.get("weather"))
        print("💨 Air quality data:", data.get("air_quality"))
        print("💧 Water data:", data.get("water"))

        print("🔁 Starting transformations...")
        transform_weather(data["weather"])
        transform_air_quality(data["air_quality"])
        transform_water(data["water"])
        print("✅ All transformations completed.")

    except Exception as e:
        print(f"❌ Error in transformation: {e}")
        raise