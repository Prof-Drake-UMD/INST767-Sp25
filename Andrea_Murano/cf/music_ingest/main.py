import os
import base64
import json
import logging
import requests
from datetime import datetime
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "music"
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

def get_spotify_token():
    """Obtains a Spotify access token using the Client Credentials flow."""
    auth_url = "https://accounts.spotify.com/api/token"

    client_creds = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    client_creds_b64 = base64.b64encode(client_creds.encode()).decode()

    headers = {
        "Authorization": f"Basic {client_creds_b64}",
        "Content-Type": "application/x-www-form-urlencoded"  
    }
    data = {"grant_type": "client_credentials"}

    logging.info("Requesting Spotify access token...")
    try:
        response = requests.post(auth_url, headers=headers, data=data, timeout=10)
        response.raise_for_status()  
        token_info = response.json()
        logging.info("Successfully obtained Spotify access token.")
        return token_info["access_token"]
    except requests.exceptions.RequestException as e:
        logging.error(f"Error obtaining Spotify token: {e}")
        raise  

def get_spotify_tracks(token, min_year=1950, limit=50):
    """Searches for Spotify tracks released from a minimum year onwards (adjust limit as needed)."""
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    all_results = []
    offset = 0
    while True:
        params = {
            "q": f"year:>={min_year}",
            "type": "track",
            "limit": limit,  
            "offset": offset
        }

        logging.info(f"Searching for Spotify tracks (offset: {offset})...")
        try:
            resp = requests.get(search_url, headers=headers, params=params, timeout=30)  
            resp.raise_for_status()  
            data = resp.json()
            tracks = data.get("tracks", {}).get("items", [])

            if not tracks:
                break  

            for track in tracks:
                all_results.append({
                    "track_id": track.get("id"),
                    "title": track.get("name"),
                    "artist": track.get("artists", [{}])[0].get("name", ""),
                    "album": track.get("album", {}).get("name", ""),
                    "release_date": track.get("album", {}).get("release_date", None),
                    "preview_url": track.get("preview_url"),
                    "ingest_ts": datetime.utcnow().isoformat()
                })
            offset += limit 
            logging.info(f"Collected {len(all_results)} tracks so far.")

            if offset >= 2000:
                logging.warning("Reached maximum offset for Spotify API.  May not retrieve ALL results.")
                break

        except requests.exceptions.RequestException as e:
            logging.error(f"Error searching for Spotify tracks: {e}")
            break  


    logging.info(f"Found a total of {len(all_results)} tracks.")
    return all_results


def write_to_bigquery(rows):
    """Writes track data to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    logging.info(f"Writing {len(rows)} rows to BigQuery table {table_id}...")
    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            logging.error(f"BigQuery insert errors: {errors}")
            raise Exception(f"BigQuery insert errors: {errors}")  

        logging.info("Successfully wrote data to BigQuery.")
    except Exception as e:
        logging.error(f"Error writing to BigQuery: {e}")
        raise  

def main(event, context):
    """Main Cloud Function entry point."""
    logging.info("Starting music ingest Cloud Function...")
    try:
        token = get_spotify_token()
        music = get_spotify_tracks(token)

        if music:
            write_to_bigquery(music)
        else:
            logging.info("No music tracks found for the specified criteria.")

        logging.info("Music ingest Cloud Function completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during music ingest: {e}")
        raise  
