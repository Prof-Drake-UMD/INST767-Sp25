import base64
import json
import os
from datetime import datetime
from google.cloud import storage
from clean_transform_movies import clean_movie

def upload_to_gcs(data, bucket_name, prefix="cleaned"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    blob = bucket.blob(f"{prefix}/movie_{timestamp}.json")

    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type="application/json"
    )
    print(f"✅ Uploaded cleaned movie to GCS: {blob.name}")

def main_entry(event, context):
    try:
        # Decode and parse Pub/Sub message
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        raw_movie = json.loads(pubsub_message)

        print("📩 Received raw movie data")

        # Clean the movie
        cleaned = clean_movie(raw_movie, is_now_playing=True)

        # Upload to GCS
        bucket_name = os.environ["GCS_BUCKET"]
        upload_to_gcs(cleaned, bucket_name)

    except Exception as e:
        print(f"❌ Error processing message: {e}")
