import os
import time
import logging
import requests
from datetime import datetime
from google.cloud import bigquery


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "artwork"

def get_met_artwork(year, buffer=10):
    """Retrieves artwork data from the Metropolitan Museum of Art API."""
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"
    object_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/"
    results = []
    artwork_count = 0 
    MET_API_DELAY = 0.5 

    for y in range(year - buffer, year + buffer + 1):
        logging.info(f"Searching for artworks from year {y}...")
        try:
            resp = requests.get(search_url, params={"q": str(y), "hasImages": True}, timeout=10)
            resp.raise_for_status() 

            ids = resp.json().get("objectIDs", []) or []
            logging.info(f"Found {len(ids)} object IDs for year {y}.")

            for object_id in ids[:3]:  
                if artwork_count >= 10: 
                    logging.info("Reached maximum artwork limit. Stopping.")
                    return results
                
                try:
                    time.sleep(MET_API_DELAY) # Respecting rate limits
                    obj_data_resp = requests.get(object_url + str(object_id), timeout=10)
                    obj_data_resp.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                    obj_data = obj_data_resp.json()

                    if obj_data and obj_data.get("objectID"): # Ensure data and objectID exist
                        try:
                            artwork_data = {
                                "object_id": int(obj_data.get("objectID")), # Convert after retrieving data
                                "title": obj_data.get("title"),
                                "artist_name": obj_data.get("artistDisplayName"),
                                "medium": obj_data.get("medium"),
                                "object_date": obj_data.get("objectDate"),
                                "object_url": obj_data.get("objectURL"),
                                "image_url": obj_data.get("primaryImageSmall"),
                                "ingest_ts": datetime.utcnow().isoformat()
                            }
                            results.append(artwork_data)
                            artwork_count += 1
                            logging.info(f"Successfully processed artwork with ID {object_id}.")
                        except (TypeError, ValueError) as e:
                            logging.warning(f"Skipping artwork {object_id} due to data conversion issue: {e}")


                    else:
                        logging.warning(f"Skipping artwork {object_id} due to missing or invalid data.")

                except requests.exceptions.RequestException as e:
                    logging.error(f"Error fetching object data for ID {object_id}: {e}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error searching for artworks from year {y}: {e}")

    logging.info(f"Retrieved {len(results)} artworks from The Met API.")
    return results

def write_to_bigquery(rows):
    """Writes artwork data to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    logging.info(f"Writing {len(rows)} rows to BigQuery table {table_id}...")

    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            logging.error(f"BigQuery insert errors: {errors}")
            raise Exception(f"BigQuery insert errors: {errors}")  # Signal function failure

        logging.info("Successfully wrote data to BigQuery.")

    except Exception as e:
        logging.error(f"Error writing to BigQuery: {e}")
        raise  # Re-raise to signal function failure

def main(event, context):
    """Main Cloud Function entry point."""
    logging.info("Starting artwork ingest Cloud Function...")
    year = 2018
    try:
        artwork = get_met_artwork(year)
        if artwork:
            write_to_bigquery(artwork)
        else:
            logging.info("No artwork found for the specified year.")

        logging.info("Artwork ingest Cloud Function completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during artwork ingest: {e}")
        raise  # Crucial: Re-raise the exception to signal function failure to Google Cloud

