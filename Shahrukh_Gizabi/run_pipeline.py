# run_pipeline.py
import os
import json
import datetime
import time # Optional for delays

# Import your existing modules
import api_client
# We will no longer call data_transformer directly from here for CSV output
# import data_transformer 
# import pandas as pd # No longer directly creating DataFrames or CSVs here

# Import Google Cloud client libraries
from google.cloud import storage
from google.cloud import pubsub_v1

# --- Configuration & API Keys (from environment variables) ---
SPORTSGAMEODDS_API_KEY = os.environ.get("SPORTSGAMEODDS_API_KEY")
FOOTBALL_DATA_ORG_TOKEN = os.environ.get("FOOTBALL_DATA_ORG_TOKEN")
API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY")

# --- NEW: GCP Configuration (from environment variables) ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_RAW_DATA_BUCKET = os.environ.get("GCS_RAW_DATA_BUCKET") # e.g., your-project-id-raw-data
PUB_SUB_TOPIC_ID = os.environ.get("PUB_SUB_TOPIC_ID")       # e.g., transform-data-requests

# Initialize GCS and Pub/Sub clients (globally or within main/functions)
storage_client = None
publisher_client = None
pubsub_topic_path = None

def initialize_gcp_clients():
    global storage_client, publisher_client, pubsub_topic_path
    if not GCP_PROJECT_ID:
        print("Error: GCP_PROJECT_ID environment variable not set.")
        return False
    if not GCS_RAW_DATA_BUCKET:
        print("Error: GCS_RAW_DATA_BUCKET environment variable not set.")
        return False
    if not PUB_SUB_TOPIC_ID:
        print("Error: PUB_SUB_TOPIC_ID environment variable not set.")
        return False
        
    try:
        storage_client = storage.Client()
        publisher_client = pubsub_v1.PublisherClient()
        pubsub_topic_path = publisher_client.topic_path(GCP_PROJECT_ID, PUB_SUB_TOPIC_ID)
        print(f"Initialized GCP clients. Raw GCS Bucket: {GCS_RAW_DATA_BUCKET}, Pub/Sub Topic Path: {pubsub_topic_path}")
        return True
    except Exception as e:
        print(f"Error initializing GCP clients: {e}")
        return False

def save_to_gcs_and_publish(data_to_save, data_type_identifier, file_prefix, original_params=None):
    """
    Saves data to GCS and publishes a notification message to Pub/Sub.
    """
    if not data_to_save:
        print(f"No data provided to save for {data_type_identifier}. Skipping GCS upload and Pub/Sub message.")
        return

    if not storage_client or not publisher_client or not pubsub_topic_path:
        print("Error: GCS or Pub/Sub clients not initialized. Cannot save or publish.")
        return

    try:
        # 1. Save raw data to GCS
        bucket = storage_client.bucket(GCS_RAW_DATA_BUCKET)
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S%f")
        # Construct a somewhat unique blob name
        blob_name_parts = [file_prefix]
        if original_params: # Add params to filename for uniqueness if provided
            for k, v in original_params.items():
                 blob_name_parts.append(f"{k}{v}")
        blob_name_parts.append(timestamp)
        blob_name = f"{data_type_identifier}/{'_'.join(blob_name_parts)}.json"
        
        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            data=json.dumps(data_to_save),
            content_type="application/json"
        )
        gcs_uri = f"gs://{GCS_RAW_DATA_BUCKET}/{blob_name}"
        print(f"Successfully saved raw {data_type_identifier} data to {gcs_uri}")

        # 2. Publish message to Pub/Sub
        message_payload = {
            "gcs_uri": gcs_uri,
            "data_type": data_type_identifier,
            "timestamp": timestamp,
            "original_params": original_params or {}
        }
        message_bytes = json.dumps(message_payload).encode("utf-8")
        
        future = publisher_client.publish(pubsub_topic_path, data=message_bytes)
        future.result() # Wait for publish to complete (optional, can be async)
        print(f"Successfully published message for {data_type_identifier} from {gcs_uri}")

    except Exception as e:
        print(f"Error in save_to_gcs_and_publish for {data_type_identifier}: {e}")


def main():
    print("Starting Ingest Orchestrator pipeline...")

    # Check for API keys
    if not all([API_FOOTBALL_KEY, FOOTBALL_DATA_ORG_TOKEN, SPORTSGAMEODDS_API_KEY]):
        print("Error: One or more API keys are not set. Exiting.")
        return
        
    if not initialize_gcp_clients():
        print("Failed to initialize GCP clients. Exiting.")
        return

    # === Task 1: Process TeamSeasonStats (from api-football.com) ===
    print("\nIngesting TeamSeasonStats...")
    try:
        params_team_stats = {"league_id": "39", "season": "2023", "team_id": "33"}
        raw_team_stats_data = api_client.fetch_api_football_team_stats(
            api_key=API_FOOTBALL_KEY,
            league_id=params_team_stats["league_id"],
            season=params_team_stats["season"],
            team_id=params_team_stats["team_id"]
        )
        save_to_gcs_and_publish(raw_team_stats_data, "team_stats", "api_football", original_params=params_team_stats)
    except Exception as e:
        print(f"Error ingesting TeamSeasonStats: {e}")

    # time.sleep(1) # Optional delay

    # === Task 2: Process Matches (from football-data.org) ===
    print("\nIngesting Matches...")
    try:
        params_matches = {"competition_code": "PL", "season": "2023"}
        raw_matches_data = api_client.fetch_football_data_matches(
            api_token=FOOTBALL_DATA_ORG_TOKEN,
            competition_code=params_matches["competition_code"],
            season=params_matches["season"]
        )
        # The matches data is under the 'matches' key, and other metadata is present.
        # For Pub/Sub, it might be better to send the whole raw_matches_data if the
        # transformer expects the full structure, or just raw_matches_data['matches']
        # if the transformer only needs the list. Let's send the whole raw object.
        save_to_gcs_and_publish(raw_matches_data, "matches", "football_data_org", original_params=params_matches)
    except Exception as e:
        print(f"Error ingesting Matches: {e}")

    # time.sleep(1) # Optional delay

    # === Task 3: Process MatchEventsAndOdds (from sportsgameodds.com) ===
    print("\nIngesting MatchEventsAndOdds...")
    try:
        # Shortened date range for less data during testing, adjust as needed
        params_odds = {
            "league_id": "EPL", 
            "starts_after": "2024-08-16", 
            "starts_before": "2024-08-20" # Fetch only a few days for testing
        }
        raw_odds_data = api_client.fetch_sports_game_odds(
            api_key=SPORTSGAMEODDS_API_KEY,
            league_id=params_odds["league_id"],
            starts_after=params_odds["starts_after"],
            starts_before=params_odds["starts_before"]
        )
        # This API's response usually has events under a 'data' key.
        # We'll save the whole raw_odds_data object.
        save_to_gcs_and_publish(raw_odds_data, "odds_events", "sportsgameodds", original_params=params_odds)
    except Exception as e:
        print(f"Error ingesting MatchEventsAndOdds: {e}")

    print("\nIngest Orchestrator pipeline finished.")

if __name__ == "__main__":
    main()
    