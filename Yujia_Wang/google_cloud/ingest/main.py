# google_cloud/ingest/main.py

from fetch import gather_movie_full_data
from google.cloud import pubsub_v1
import os
import json

def main_entry(request):
    topic_name = os.environ.get("TOPIC_NAME")
    bucket_name = os.environ.get("GCS_BUCKET")
    project_id = os.environ.get("PROJECT_ID")

    # Step 1: Fetch full movie list and save to GCS (done inside fetch.py)
    movies = gather_movie_full_data(bucket_name=bucket_name, region='US')

    # Step 2: Publish full list as a single JSON message
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    full_payload = json.dumps(movies).encode("utf-8")
    publisher.publish(topic_path, full_payload)

    return f"✅ Published full movie list of {len(movies)} movies to Pub/Sub topic: {topic_name}"
