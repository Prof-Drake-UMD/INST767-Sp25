
from google.cloud import storage
import pandas as pd
import io
import json

from io import StringIO

def store_json_to_gcs(json_data, bucket, prefix, file_name):
    
    # Convert dict to JSON string
    json_string = json.dumps(json_data, indent=2)

    # Upload to GCS
    client = storage.Client()
    bucket = client.bucket(bucket)

    raw_location = f'{prefix}/{file_name}.json'
    blob = bucket.blob(raw_location)

    blob.upload_from_string(json_string, content_type="application/json")

    print("✅ Raw JSON uploaded to GCS successfully.")

def store_df_to_gcs(df, bucket, prefix, file_name):
# Initialize the Google Cloud Storage client
    storage_client = storage.Client()

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # # Initialize GCS client
    # client = storage.Client()

    # Upload the CSV from memory
    

    # print("✅ DataFrame uploaded to GCS successfully.")

    # Get the Google Cloud Storage bucket object
    bucket = storage_client.bucket(bucket)
    gcs_raw_location = f'{prefix}/{file_name}.csv'
    # Create a blob (a file object) in the bucket
    blob = bucket.blob(gcs_raw_location)
    blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    # # Upload the CSV data to the bucket from memory
    # blob.upload_from_string(csv_data, content_type='text/csv')

    print(f"DataFrame successfully uploaded to {gcs_raw_location}")