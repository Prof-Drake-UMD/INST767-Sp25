import os
import requests
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "music"
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

def get_spotify_token():
    auth_url = "https://accounts.spotify.com/api/token"
    data = {"grant_type": "client_credentials"}
    response = requests.post(auth_url, data=data, auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET), timeout=10)
    response.raise_for_status()
    return response.json()["access_token"]

def get_spotify_tracks(year, token, limit=5):
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    results = []
    params = {
        "q": f"year:{year}",
        "type": "track",
        "limit": 20
    }
    resp = requests.get(search_url, headers=headers, params=params, timeout=10)
    if resp.status_code == 200:
        for track in resp.json().get("tracks", {}).get("items", []):
            results.append({
                "track_id": track.get("id"),
                "title": track.get("name"),
                "artist": track.get("artists", [{}])[0].get("name", ""),
                "album": track.get("album", {}).get("name", ""),
                "release_date": track.get("album", {}).get("release_date", None),
                "preview_url": track.get("preview_url"),
                "ingest_ts": datetime.utcnow().isoformat()
            })
    return results[:limit]

def write_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("BigQuery error:", errors)

def main(event, context):
    year = 2018  
    token = get_spotify_token()
    music = get_spotify_tracks(year, token)
    if music:
        write_to_bigquery(music)
