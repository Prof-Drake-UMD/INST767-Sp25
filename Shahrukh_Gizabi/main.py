# main.py
import os
import json
import base64
import datetime
import functions_framework 
from google.cloud import storage
from google.cloud import bigquery 
import pandas as pd
import time 

import data_transformer

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_PROCESSED_DATA_BUCKET = os.environ.get("GCS_PROCESSED_DATA_BUCKET")
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "soccer_premier_league_analytics") # Default if not set

storage_client = storage.Client()
bigquery_client = bigquery.Client() 

@functions_framework.cloud_event 
def unified_transformer_pubsub(cloud_event):
    start_time_processing = time.time() 
    
    event_id = cloud_event.get('id', 'N/A') 
    event_type = cloud_event.get('type', 'N/A') 
    print(f"Unified Transformer: Received CloudEvent ID: {event_id}")
    print(f"Unified Transformer: CloudEvent type: {event_type}")

    if not all([GCS_PROCESSED_DATA_BUCKET, GCP_PROJECT_ID, BIGQUERY_DATASET_ID]):
        print("Error: Missing one or more critical environment variables (GCS_PROCESSED_DATA_BUCKET, GCP_PROJECT_ID, BIGQUERY_DATASET_ID). Exiting.")
        return "Configuration error: Missing GCS/BigQuery env vars.", 500

    gcs_uri_raw_json = None 
    data_type = "unknown"  

    try:
        message_payload = cloud_event.data.get("message")
        if not message_payload:
            print(f"Warning (CloudEvent ID: {event_id}): 'message' attribute missing in CloudEvent data. Exiting.")
            return "Message attribute missing.", 200 

        message_data_encoded = message_payload.get("data")
        if not message_data_encoded:
            print(f"Warning (CloudEvent ID: {event_id}): No 'data' attribute found in Pub/Sub message payload. Exiting.")
            return "No data in Pub/Sub message.", 200 

        message_data_str = base64.b64decode(message_data_encoded).decode("utf-8")
        message_data = json.loads(message_data_str)
        
        gcs_uri_raw_json = message_data.get("gcs_uri")
        data_type = message_data.get("data_type", "unknown") 
        original_params = message_data.get("original_params", {})
        
        print(f"Processing message (CloudEvent ID: {event_id}) for data_type: '{data_type}', Raw JSON GCS URI: '{gcs_uri_raw_json}'")

        if not gcs_uri_raw_json:
            print(f"Error (CloudEvent ID: {event_id}): gcs_uri not found in Pub/Sub message. Exiting.")
            return "gcs_uri missing.", 400

        if not gcs_uri_raw_json.startswith("gs://"):
            raise ValueError(f"Invalid GCS URI format for raw JSON: {gcs_uri_raw_json}")
        
        path_parts = gcs_uri_raw_json.replace("gs://", "").split("/", 1)
        source_bucket_name = path_parts[0]
        source_blob_name = path_parts[1]

        source_bucket = storage_client.bucket(source_bucket_name)
        blob = source_bucket.blob(source_blob_name)
        
        print(f"Attempting to download: {gcs_uri_raw_json} (CloudEvent ID: {event_id})")
        raw_data_json_str = blob.download_as_text(timeout=120) 
        raw_api_response_data = json.loads(raw_data_json_str)
        print(f"Successfully downloaded raw data for {data_type} from {gcs_uri_raw_json} (CloudEvent ID: {event_id})")

        df_transformed = pd.DataFrame()
        output_filename_prefix = "unknown_type"
        bigquery_table_id_short = None 
        
        if data_type == "team_stats":
            df_transformed = data_transformer.transform_team_season_stats_to_df(raw_api_response_data)
            output_filename_prefix = "TeamSeasonStats"
            bigquery_table_id_short = "TeamSeasonStats"
        elif data_type == "matches":
            df_transformed = data_transformer.transform_matches_to_df(raw_api_response_data)
            output_filename_prefix = "Matches"
            bigquery_table_id_short = "Matches"
        elif data_type == "odds_events":
            df_transformed = data_transformer.transform_match_events_odds_to_df(raw_api_response_data)
            output_filename_prefix = "MatchEventsAndOdds"
            bigquery_table_id_short = "MatchEventsAndOdds" 
        else:
            print(f"Warning (CloudEvent ID: {event_id}): Unknown data_type '{data_type}'. No transformation applied.")
            return f"Unknown data_type: {data_type}", 200

        if not df_transformed.empty:

            unique_part_of_filename = "data"
            try:
                unique_part_of_filename = os.path.basename(source_blob_name).split('_')[-1].replace('.json','')
            except Exception:
                print(f"Warning: Could not parse unique part from filename {source_blob_name}, using default.")

            csv_filename = f"{output_filename_prefix}_{unique_part_of_filename}.csv"
            
            destination_bucket = storage_client.bucket(GCS_PROCESSED_DATA_BUCKET)
            gcs_path_to_csv = f"{data_type}/{csv_filename}"
            csv_blob = destination_bucket.blob(gcs_path_to_csv)
            
            print(f"Attempting to upload transformed CSV to gs://{GCS_PROCESSED_DATA_BUCKET}/{gcs_path_to_csv} (CloudEvent ID: {event_id})")
            csv_blob.upload_from_string(
                df_transformed.to_csv(index=False, encoding='utf-8-sig'),
                content_type="text/csv"
            )
            gcs_uri_csv = f"gs://{GCS_PROCESSED_DATA_BUCKET}/{gcs_path_to_csv}"
            print(f"Successfully transformed and saved {data_type} to {gcs_uri_csv} (CloudEvent ID: {event_id})")

            if bigquery_table_id_short:

                table_ref_str = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{bigquery_table_id_short}"
                print(f"Attempting to load data from {gcs_uri_csv} into BigQuery table {table_ref_str} (CloudEvent ID: {event_id})")
                
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.CSV
                job_config.skip_leading_rows = 1 
                job_config.autodetect = False   
                
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND # Or WRITE_TRUNCATE

                load_job = bigquery_client.load_table_from_uri(
                    gcs_uri_csv,
                    table_ref_str, # Target table
                    job_config=job_config
                )
                load_job.result(timeout=180) 
                
                if load_job.errors:
                    print(f"BigQuery load job finished with errors for {table_ref_str} (CloudEvent ID: {event_id}): {load_job.errors}")
                    return f"BigQuery load errors: {load_job.errors}", 500 
                else:
                    destination_table = bigquery_client.get_table(table_ref_str)
                    print(f"Successfully loaded {destination_table.num_rows} rows into BigQuery table {table_ref_str} (CloudEvent ID: {event_id})")
            else:
                print(f"No BigQuery table ID defined for data_type '{data_type}'. Skipping BigQuery load. (CloudEvent ID: {event_id})")

            processing_time = time.time() - start_time_processing
            print(f"Total processing time for {data_type} (CloudEvent ID: {event_id}): {processing_time:.2f} seconds.")
            return "Transformation and BigQuery load successful.", 200
        else:
            print(f"Transformation resulted in an empty DataFrame for {data_type} ({gcs_uri_raw_json}, CloudEvent ID: {event_id}). No CSV saved, no BQ load.")
            return f"Empty transformation for {data_type}", 200

    except ValueError as ve:
        print(f"ValueError during processing (CloudEvent ID: {event_id}, GCS URI: {gcs_uri_raw_json}, Data Type: {data_type}): {ve}")
        return f"ValueError: {ve}", 400 
    except Exception as e:
        print(f"Unhandled error processing message (CloudEvent ID: {event_id}, GCS URI: {gcs_uri_raw_json}, Data Type: {data_type}): {e}")
        import traceback
        traceback.print_exc() 
        raise e