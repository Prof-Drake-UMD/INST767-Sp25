import base64
import re
from datetime import datetime
import logging
import requests
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define project constants
PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "artwork"

def get_met_artwork():
    """Retrieves artwork data from the Metropolitan Museum of Art API."""
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    object_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/"
    results = []
    MET_API_DELAY = 0.5

    logging.info("Retrieving all object IDs from The Met API...")
    try:
        resp = requests.get(search_url, timeout=10)
        resp.raise_for_status()
        object_ids = resp.json().get("objectIDs", []) or []
        logging.info(f"Found {len(object_ids)} object IDs.")

        for object_id in object_ids:
            try:
                time.sleep(MET_API_DELAY)
                obj_data_resp = requests.get(object_url + str(object_id), timeout=10)
                obj_data_resp.raise_for_status()
                obj_data = obj_data_resp.json()

                if obj_data and obj_data.get("objectID"):
                    try:
                        object_date_str = obj_data.get("objectDate")
                        year = None

                        if object_date_str:
                            match = re.search(r'(\d{4})', object_date_str)
                            if match:
                                year = int(match.group(1))

                        artwork_data = {
                            "object_id": int(obj_data.get("objectID")),
                            "title": obj_data.get("title"),
                            "artist_name": obj_data.get("artistDisplayName"),
                            "medium": obj_data.get("medium"),
                            "object_date": obj_data.get("objectDate"),
                            "object_year": year,  # Store the extracted year
                            "object_url": obj_data.get("objectURL"),
                            "image_url": obj_data.get("primaryImageSmall"),
                            "ingest_ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                        }
                        results.append(artwork_data)
                        logging.info(f"Successfully processed artwork with ID {object_id}.")

                    except (TypeError, ValueError) as e:
                        logging.warning(f"Skipping artwork {object_id} due to data conversion issue: {e}")
                else:
                    logging.warning(f"Skipping artwork {object_id} due to missing or invalid data.")

            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching object data for ID {object_id}: {e}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error retrieving object IDs: {e}")

    logging.info(f"Retrieved {len(results)} artworks from The Met API.")
    return results

def write_to_bigquery(rows):
    """Writes artwork data to BigQuery with retry logic."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{DATASET}.{TABLE}"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logging.info(f"Writing {len(rows)} rows to BigQuery table {table_id}, attempt {attempt + 1}...")

            # Convert None values to None (for BigQuery)
            for row in rows:
                for key, value in row.items():
                    if value is None:
                        row[key] = None  # Explicitly set to None

            errors = client.insert_rows_json(table_id, rows)
            if errors:
                logging.error(f"BigQuery insert errors: {errors}")
                raise Exception(f"BigQuery insert errors: {errors}")  
            logging.info("Successfully wrote data to BigQuery.")
            return  
        except Exception as e:
            logging.error(f"Error writing to BigQuery (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5) 
            else:
                logging.error("Max retries reached.  Failing.")
                raise  

def main(event, context):
    """Main Cloud Function entry point for Pub/Sub trigger."""
    logging.info("Starting artwork ingest Cloud Function...")
    try:
        if 'data' in event:
            message = base64.b64decode(event['data']).decode('utf-8')
            logging.info(f"Received message: {message}")
        else:
            logging.warning("No data found in Pub/Sub message.")

        artwork = get_met_artwork()
        num_artworks = len(artwork)
        logging.info(f"Retrieved {num_artworks} artworks from The Met API.")
        if artwork:
            write_to_bigquery(artwork)
            logging.info(f"Successfully wrote {num_artworks} artworks to BigQuery.")
        else:
            logging.info("No artwork found for the specified criteria.")

        logging.info("Artwork ingest Cloud Function completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during artwork ingest: {e}")
        raise
