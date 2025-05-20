import os
import requests
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "artworks"

def get_met_artworks(year, buffer=10):
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"
    object_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/"
    results = []
    for y in range(year - buffer, year + buffer + 1):
        resp = requests.get(search_url, params={"q": str(y), "hasImages": True}, timeout=10)
        if resp.status_code != 200:
            continue
        ids = resp.json().get("objectIDs", []) or []
        for object_id in ids[:3]:
            obj_data = requests.get(object_url + str(object_id), timeout=10).json()
            if obj_data:
                results.append({
                    "object_id": int(obj_data.get("objectID")),
                    "title": obj_data.get("title"),
                    "artist_name": obj_data.get("artistDisplayName"),
                    "medium": obj_data.get("medium"),
                    "object_date": obj_data.get("objectDate"),
                    "object_url": obj_data.get("objectURL"),
                    "image_url": obj_data.get("primaryImageSmall"),
                    "ingest_ts": datetime.utcnow().isoformat()
                })
    return results

def write_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("BigQuery error:", errors)

def main(event, context):
    year = 2018  # could be parameterized from Pub/Sub or event trigger
    artworks = get_met_artworks(year)
    if artworks:
        write_to_bigquery(artworks)
