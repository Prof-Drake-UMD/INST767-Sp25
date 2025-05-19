from google.cloud import pubsub_v1
import json
from ingest.api_calls import fetch_weather_data, fetch_air_quality_data, fetch_worldbank_data
from load.bigquery_loader import load_json_to_bq
from transform.transform_data import transform_weather_data, transform_air_quality_data, transform_gdp_data

# Initialize Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('climate-data-pipeline-457720', 'climate-data-sub')

def callback(message):
    print(f"✅ Received message: {message.data}")
    payload = json.loads(message.data)

    if payload["type"] == "weather":
        transformed = transform_weather_data([payload["data"]])
        load_json_to_bq("climate_data", "weather", transformed)

    elif payload["type"] == "air_quality":
        transformed = transform_air_quality_data([payload["data"]])
        load_json_to_bq("climate_data", "air_quality", transformed)

    elif payload["type"] == "gdp":
        transformed = transform_gdp_data(payload["data"])
        load_json_to_bq("climate_data", "gdp", transformed)

    message.ack()

def listen_for_messages():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print("✅ Listening for messages on Pub/Sub...")
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    listen_for_messages()
