import base64
import json
import os
from datetime import datetime
from google.cloud import storage
from clean_transform_movies import clean_movies_list

def upload_to_gcs(data, bucket_name, prefix="cleaned"):
    client = storage.Client()
    bucket = bucket = client.bucket(bucket_name)

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    blob = bucket.blob(f"{prefix}/movies_{timestamp}.json")

    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type="application/json"
    )
    print(f"✅ Uploaded cleaned movie list to GCS: {blob.name}")

def main_entry(event, context):
    try:
        # Decode the entire movie list from Pub/Sub
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        raw_movies = json.loads(pubsub_message)

        print(f"📩 Received {len(raw_movies)} raw movies")

        # Clean all movies at once
        cleaned_movies = clean_movies_list(raw_movies, is_now_playing=True)

        # Upload cleaned batch
        bucket_name = os.environ["GCS_BUCKET"]
        upload_to_gcs(cleaned_movies, bucket_name)

    except Exception as e:
        print(f"❌ Error processing message: {e}")
