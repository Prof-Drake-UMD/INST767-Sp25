# main.py (Corrected version)
import os
import json
import base64
import datetime
import functions_framework
from google.cloud import storage
import pandas as pd
import time 
import data_transformer

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_PROCESSED_DATA_BUCKET = os.environ.get("GCS_PROCESSED_DATA_BUCKET")

storage_client = storage.Client()

@functions_framework.cloud_event
def unified_transformer_pubsub(cloud_event):
    start_time_processing = time.time() 
    print(f"Unified Transformer: Received CloudEvent ID: {cloud_event['id']}") 
    print(f"Unified Transformer: CloudEvent type: {cloud_event['type']}")

    if not GCS_PROCESSED_DATA_BUCKET:
        print("Error: GCS_PROCESSED_DATA_BUCKET environment variable not set. Exiting.")
        return "Configuration error: GCS_PROCESSED_DATA_BUCKET not set.", 500

    try:
        message_payload = cloud_event.data.get("message")
        if not message_payload:
            print("Warning: 'message' attribute missing in CloudEvent data. Exiting.")
            return "Message attribute missing.", 200 # Acknowledge

        message_data_encoded = message_payload.get("data")
        if not message_data_encoded:
            print("Warning: No 'data' attribute found in Pub/Sub message payload. Exiting.")
            return "No data in Pub/Sub message.", 200 # Acknowledge

        message_data_str = base64.b64decode(message_data_encoded).decode("utf-8")
        message_data = json.loads(message_data_str)
        
        gcs_uri = message_data.get("gcs_uri")
        data_type = message_data.get("data_type")
        
        print(f"Processing message for data_type: '{data_type}', GCS URI: '{gcs_uri}'")

        if not gcs_uri:
            print("Error: gcs_uri not found in Pub/Sub message. Exiting.")
            return "gcs_uri missing.", 400

        if not gcs_uri.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI format: {gcs_uri}")
        
        path_parts = gcs_uri.replace("gs://", "").split("/", 1)
        source_bucket_name = path_parts[0]
        source_blob_name = path_parts[1]

        source_bucket = storage_client.bucket(source_bucket_name)
        blob = source_bucket.blob(source_blob_name)
        
        print(f"Attempting to download: {gcs_uri}")
        raw_data_json_str = blob.download_as_text(timeout=120)
        raw_api_response_data = json.loads(raw_data_json_str)
        print(f"Successfully downloaded raw data for {data_type} from {gcs_uri}")

        df_transformed = pd.DataFrame()
        output_filename_prefix = "unknown_type"

        if data_type == "team_stats":
            df_transformed = data_transformer.transform_team_season_stats_to_df(raw_api_response_data)
            output_filename_prefix = "TeamSeasonStats"
        elif data_type == "matches":
            df_transformed = data_transformer.transform_matches_to_df(raw_api_response_data)
            output_filename_prefix = "Matches"
        elif data_type == "odds_events":
            df_transformed = data_transformer.transform_match_events_odds_to_df(raw_api_response_data)
            output_filename_prefix = "MatchEventsAndOdds"
        else:
            print(f"Warning: Unknown data_type '{data_type}'. No transformation applied.")
            return f"Unknown data_type: {data_type}", 200

        if not df_transformed.empty:
            timestamp_from_gcs_filename = os.path.basename(source_blob_name).split('_')[-1].replace('.json','')
            csv_filename = f"{output_filename_prefix}_{timestamp_from_gcs_filename}.csv"
            
            destination_bucket = storage_client.bucket(GCS_PROCESSED_DATA_BUCKET)
            csv_blob = destination_bucket.blob(f"{data_type}/{csv_filename}")
            
            print(f"Attempting to upload transformed CSV to gs://{GCS_PROCESSED_DATA_BUCKET}/{data_type}/{csv_filename}")
            csv_blob.upload_from_string(
                df_transformed.to_csv(index=False, encoding='utf-8-sig'),
                content_type="text/csv"
            )
            print(f"Successfully transformed and saved {data_type} to gs://{GCS_PROCESSED_DATA_BUCKET}/{data_type}/{csv_filename}")
            processing_time = time.time() - start_time_processing
            print(f"Total processing time for {data_type} (CloudEvent ID: {cloud_event['id']}): {processing_time:.2f} seconds.")
            return "Transformation successful, CSV saved to GCS.", 200
        else:
            print(f"Transformation resulted in an empty DataFrame for {data_type} ({gcs_uri}). No CSV saved.")
            return f"Empty transformation for {data_type}", 200

    except ValueError as ve:
        print(f"ValueError during processing (CloudEvent ID: {cloud_event.get('id', 'N/A')}): {ve}")
        return f"ValueError: {ve}", 400 
    except Exception as e:
        print(f"Unhandled error processing message (CloudEvent ID: {cloud_event.get('id', 'N/A')}): {e}")
        import traceback
        traceback.print_exc()
        raise e