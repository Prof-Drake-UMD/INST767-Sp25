import base64
import json
import pandas as pd
import io
from google.cloud import storage
import os
from google.cloud import pubsub_v1

def fetch_and_upload(event, context):
    combined_file = False
    # Decode the Pub/Sub message
    if "data" not in event:
        print("No data in event")
        return

    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    message_json = json.loads(pubsub_message)

    file_name = message_json.get("name")
    bucket_name = "api_output_bucket_inst_final"

    print(f"Received file upload event for: {file_name}")

    # Only process files that start with "metro" and end with .csv
    if not file_name.startswith("metro") or not file_name.endswith(".csv"):
        print("Not a metro CSV file — still running combination")

    if file_name.startswith('combined'):
        combined_file=True
        print("Combined file detected")

    # Combine all matching metro*.csv files in the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    # Only process files in the "output/" folder
    if not file_name.startswith("output/"):
        print(f"Ignoring file outside output/: {file_name}")
        if combined_file:
            print("Triggering bigquery script")
            #call a pubsub to insert to big query 
            project_id = "instfinal-459621"
            topic_id = "transformation-complete"

            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(project_id, topic_id)

            #dont sent data just blsnk message
            message = b'{}'

            future = publisher.publish(topic_path, message)
            print(f"Published message ID: {future.result()}")
        return

    metro_blobs = [b for b in blobs if b.name.startswith("output/metro") and b.name.endswith(".csv")]
    if not metro_blobs:
        print("No metro CSV files to combine.")
        return

    dataframes = []
    for i, blob in enumerate(metro_blobs):
        content = blob.download_as_text()
        df = pd.read_csv(io.StringIO(content))
        if i != 0:
            df.columns = dataframes[0].columns  # Normalize headers
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)

    # Write combined CSV back to GCS
    output_blob_name = f"combined_output/combined_metro.csv"
    output_blob = bucket.blob(output_blob_name)
    output_blob.upload_from_string(combined_df.to_csv(index=False), content_type="text/csv")

    #move data
    source_weather = bucket.blob("output/weather_api_output.csv")
    dest_weather = bucket.copy_blob(source_weather, bucket, 'combined_output/final_weather.csv')

    source_traffic = bucket.blob("output/traffic.csv")
    dest_traffic = bucket.copy_blob(source_traffic, bucket, 'combined_output/final_traffic.csv')



    print(f"✅✅ Combined CSV written to {output_blob_name}")
