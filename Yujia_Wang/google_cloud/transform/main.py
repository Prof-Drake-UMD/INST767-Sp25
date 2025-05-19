import base64
import json
import os
from datetime import datetime
from google.cloud import storage
from clean_transform_movies import clean_movies_list
from load_now_playing import load_json_to_bigquery
from schema import NOW_PLAYING_SCHEMA

def upload_to_gcs(data, bucket_name, prefix="cleaned"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    blob_path = f"{prefix}/movie_{timestamp}.json"
    blob = bucket.blob(blob_path)

    ndjson_string = "\n".join([json.dumps(row) for row in data])

    blob.upload_from_string(
        data=ndjson_string,
        content_type="application/json"
    )

    print(f"✅ Uploaded cleaned NDJSON to GCS: {blob.name}")
    
    # ✅ Immediately load into BigQuery
    load_json_to_bigquery(
      bucket_name=bucket_name,
      blob_path=blob_path,
      dataset_id="movies_dataset",
      table_id="now_playing_movies"
    )


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
