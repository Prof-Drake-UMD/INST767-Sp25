import os, json, requests, functions_framework
from google.cloud import pubsub_v1
from helper.store_to_gcs   import store_json_to_gcs
from helper.secret_manager import get_secret

@functions_framework.http
def extract_weather(request):
    project   = os.environ.get("GCP_PROJECT")
    bucket    = os.environ.get("RAW_BUCKET")
    raw_topic = os.environ.get("RAW_TOPIC")
    if not all((project, bucket, raw_topic)):
        missing = [k for k in ("GCP_PROJECT","RAW_BUCKET","RAW_TOPIC") if not os.environ.get(k)]
        raise RuntimeError(f"Missing env-vars: {missing}")

    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, raw_topic)
    key        = get_secret("openweather-key")
    url        = (
        "https://api.openweathermap.org/data/2.5/forecast"
        f"?lat=38.989697&lon=-76.937759&units=metric&cnt=40&appid={key}"
    )
    data = requests.get(url).json()

    store_json_to_gcs(data, bucket, "raw", "weather-openweather")
    msg = {"bucket":bucket, "path":"raw/weather-openweather.json", "domain":"weather"}
    publisher.publish(topic_path, json.dumps(msg).encode("utf-8"))

    return ("OK", 200)
