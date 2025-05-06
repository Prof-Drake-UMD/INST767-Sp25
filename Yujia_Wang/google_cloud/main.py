import json
import os
from datetime import datetime
from google.cloud import storage
from google_cloud.ingest.fetch import gather_movie_full_data
from google_cloud.transform.clean_transform_movies import clean_movie

def upload_to_gcs(data, bucket_name, filename):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type='application/json'
    )
    print(f"✅ Uploaded {filename} to gs://{bucket_name}/")
def download_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()
    return json.loads(content)
def main_entry(request):
    bucket_name = os.environ["GCS_BUCKET"]
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

    try:
        now_raw = gather_movie_full_data(region='US')
        backup_filename = f"now_playing/last_success.json"
        upload_to_gcs(now_raw, bucket_name, backup_filename)
    except RuntimeError as e:
        print(f"⚠️ {str(e)} Using previous backup instead...")
        now_raw = download_from_gcs(bucket_name, "now_playing/last_success.json")

    now_clean = [clean_movie(m, is_now_playing=True) for m in now_raw]
    upload_to_gcs(now_clean, bucket_name, f"now_playing/now_playing_{timestamp}.json")
    return f"✅ Processed {len(now_clean)} movies"
