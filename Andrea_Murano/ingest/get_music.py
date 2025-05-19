import requests
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_spotify_token():
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise ValueError("Spotify credentials not set in .env")
    auth_url = "https://accounts.spotify.com/api/token"
    resp = requests.post(auth_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=10
    )
    if resp.status_code != 200:
        raise RuntimeError("Failed to get Spotify token")
    return resp.json()["access_token"]

def get_spotify_tracks_by_year(year, token, limit=10):
    if not token:
        raise ValueError("Spotify token required")
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    results = []
    params = {"q": f"year:{year}", "type": "track", "limit": limit}
    try:
        resp = requests.get(search_url, headers=headers, params=params, timeout=10)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 1))
            print(f"Rate limited by Spotify. Retrying after {retry_after}s...")
            time.sleep(retry_after)
            return get_spotify_tracks_by_year(year, token, limit)
        elif resp.status_code != 200:
            print(f"Spotify API error: {resp.status_code}")
            return []
        tracks = resp.json().get("tracks", {}).get("items", [])
        for track in tracks:
            title = track.get("name", "")
            if not all(ord(c) < 128 for c in title):  
                continue
            results.append({
                "track_id": track.get("id"),
                "title": title,
                "artist": track.get("artists", [{}])[0].get("name", ""),
                "album": track.get("album", {}).get("name", ""),
                "release_date": track.get("album", {}).get("release_date", ""),
                "preview_url": track.get("preview_url"),
                "ingest_ts": datetime.utcnow().isoformat()
            })
    except Exception as e:
        print(f"Music API exception: {e}")
    return results
