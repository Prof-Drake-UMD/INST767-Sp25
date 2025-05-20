from google.cloud import storage
import json

def load_json_from_gcs(bucket, file_path):
    # Initialize the GCS client
    client = storage.Client()

    # Access the bucket and blob
    bucket = client.bucket(bucket)
    blob = bucket.blob(file_path)

    # Download the content as a string and parse it
    json_data = json.loads(blob.download_as_text())

    print("✅ JSON loaded from GCS successfully.")
    return json_data
