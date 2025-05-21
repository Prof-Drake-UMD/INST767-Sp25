import base64
import logging
import requests
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "artwork"
MAX_OBJECTS_PER_RUN = 1000

# Allowed years (as strings)
ALLOWED_YEARS = {str(y) for y in range(1950, 2026)}  # 1950 to 2025 inclusive

def object_date_allowed(date_str):
    """Return True if date_str matches any allowed year exactly (as string)."""
    if not date_str:
        return False
    return date_str.strip() in ALLOWED_YEARS

def get_met_artwork():
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
    object_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/"
    results = []
    try:
        resp = requests.get(search_url, timeout=60)
        resp.raise_for_status()
        object_ids = resp.json().get("objectIDs", [])[:MAX_OBJECTS_PER_RUN]
        for object_id in object_ids:
            try:
                obj_data = requests.get(object_url + str(object_id), timeout=15).json()
                obj_date = obj_data.get("objectDate", "")
                if object_date_allowed(obj_date):
                    results.append({
                        "object_id": int(obj_data.get("objectID", 0)),
                        "title": obj_data.get("title"),
                        "artist_name": obj_data.get("artistDisplayName"),
                        "medium": obj_data.get("medium"),
                        "object_date": obj_date,
                        "object_url": obj_data.get("objectURL"),
                        "image_url": obj_data.get("primaryImageSmall"),
                        "ingest_ts": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                    })
            except Exception as e:
                logging.warning(f"Error with object {object_id}: {e}")
    except Exception as e:
        logging.error(f"Error fetching object IDs: {e}")
    return results

def write_to_bigquery(rows):
    if not rows:
        logging.info("No rows to write to BigQuery.")
        return
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        logging.error(f"BigQuery insert errors: {errors}")
    else:
        logging.info(f"Successfully wrote {len(rows)} rows to BigQuery.")

def main(event, context):
    logging.info("Starting artwork ingest Cloud Function.")
    try:
        if 'data' in event:
            msg = base64.b64decode(event['data']).decode('utf-8')
            logging.info(f"Received Pub/Sub message: {msg}")
        artworks = get_met_artwork()
        write_to_bigquery(artworks)
    except Exception as e:
        logging.error(f"Function error: {e}")
        raise
