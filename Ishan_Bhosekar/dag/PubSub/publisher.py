from google.cloud import pubsub_v1
import json
from ingest.api_calls import fetch_weather_data, fetch_air_quality_data, fetch_worldbank_data
from load.bigquery_loader import load_json_to_bq
from transform.transform_data import transform_weather_data, transform_air_quality_data, transform_gdp_data
from airflow.models import Variable



# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('climate-data-pipeline-457720', 'climate-data-topic')

# Cities and countries
CITIES = [
    "New York", "Toronto", "London", "Beijing", "Tokyo", "Berlin", "Paris",
    "Mumbai", "São Paulo", "Moscow", "Sydney", "Rome", "Mexico City",
    "Seoul", "Johannesburg", "Jakarta", "Riyadh", "Istanbul", "Madrid", "Lagos"
]
COUNTRIES = [
    "US", "CA", "GB", "CN", "JP", "DE", "FR",
    "IN", "BR", "RU", "AU", "IT", "MX",
    "KR", "ZA", "ID", "SA", "TR", "ES", "NG"
]

OPENWEATHER_API_KEY = Variable.get("OPENWEATHER_API_KEY")
WAQI_TOKEN = Variable.get("WAQI_TOKEN")

def publish_messages():
    # Fetch and publish weather data
    for city in CITIES:
        weather_data = fetch_weather_data(city, OPENWEATHER_API_KEY)
        publisher.publish(topic_path, json.dumps({"type": "weather", "data": weather_data}).encode("utf-8"))

    # Fetch and publish air quality data
    for city in CITIES:
        air_data = fetch_air_quality_data(city, WAQI_TOKEN)
        publisher.publish(topic_path, json.dumps({"type": "air_quality", "data": air_data}).encode("utf-8"))

    # Fetch and publish GDP data
    for country in COUNTRIES:
        gdp_data = fetch_worldbank_data(country, "NY.GDP.MKTP.CD")
        publisher.publish(topic_path, json.dumps({"type": "gdp", "data": gdp_data}).encode("utf-8"))

    print("✅ Published all messages to Pub/Sub.")


if __name__ == "__main__":
    publish_messages()
