# main.py
from ingest import fetch_weather_forecast, fetch_air_quality, fetch_usgs_water
from transform import transform_weather, transform_air_quality, transform_water
from google.cloud import pubsub_v1
import json
from flask import Request
import base64
from google.cloud import bigquery


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

    print("🔍 Final message to publish:", message)

    future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
    print(f"✅ Published message to Pub/Sub: {future.result()}")

    return "✅ Ingest function completed"


def run_transform(event, context):
    print("📦 Raw event received:", event)

    try:
        if "data" not in event:
            raise ValueError("No 'data' field in Pub/Sub message.")

        decoded_str = base64.b64decode(event["data"]).decode("utf-8")
        if not decoded_str.strip():
            raise ValueError("Decoded message is empty.")

        data = json.loads(decoded_str)
        print("📊 Parsed JSON object:", data)

        print("🔁 Starting transformations...")
        transformed_weather = transform_weather(data["weather"])
        transformed_air = transform_air_quality(data["air_quality"])
        transformed_water = transform_water(data["water"])
        print("✅ All transformations completed.")

        for table_name, rows in {
            "weather": transformed_weather,
            "air_quality": transformed_air,
            "water": transformed_water
        }.items():
            insert_into_bigquery("dc_env_data", table_name, rows)

    except Exception as e:
        print(f"❌ Error in transformation: {e}")
        raise


bq_client = bigquery.Client()

def insert_into_bigquery(dataset_id, table_name, rows_to_insert):
    table_id = f"dc-env-project-460403.{dataset_id}.{table_name}"
    print(f"📤 Inserting {len(rows_to_insert)} rows into {table_name}...")
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"❌ Errors inserting into {table_name}: {errors}")
    else:
        print(f"✅ Inserted {len(rows_to_insert)} rows into {table_name}.")


# Optional: for one-time setup
def create_dataset_and_tables():
    client = bigquery.Client()
    dataset_id = "dc_env_data"  # <-- Change this if needed
    project = client.project
    dataset_ref = bigquery.Dataset(f"{project}.{dataset_id}")

    # Create dataset if it doesn't exist
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset {dataset_id} already exists.")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"✅ Created dataset: {dataset_id}")

    tables = {
        "weather": [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("temperature_c", "FLOAT"),
            bigquery.SchemaField("precipitation_mm", "FLOAT"),
            bigquery.SchemaField("wind_speed_mps", "FLOAT"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
],
        "air_quality": [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("pm10", "FLOAT"),
            bigquery.SchemaField("pm2_5", "FLOAT"),
            bigquery.SchemaField("carbon_monoxide", "FLOAT"),
            bigquery.SchemaField("nitrogen_dioxide", "FLOAT"),
            bigquery.SchemaField("ozone", "FLOAT"),
            bigquery.SchemaField("sulphur_dioxide", "FLOAT"),
],
        "water": [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("site_id", "STRING"),
            bigquery.SchemaField("site_name", "STRING"),
            bigquery.SchemaField("streamflow_cfs", "FLOAT"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
],
    }

    for table_name, schema in tables.items():
        table_id = f"dc-env-project-460403.{dataset_id}.{table_name}"
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table, exists_ok=True)
        print(f"📦 Table {table_name} created or already exists.")


if __name__ == "__main__":
    create_dataset_and_tables()