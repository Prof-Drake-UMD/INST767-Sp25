import requests
from datetime import datetime

def get_spotify_tracks_by_year(year, token, limit=10):
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    results = []

    params = {
        "q": f"year:{year}",
        "type": "track",
        "limit": limit
    }

    resp = requests.get(search_url, headers=headers, params=params)
    if resp.status_code == 200:
        tracks = resp.json().get("tracks", {}).get("items", [])
        for track in tracks:
            title = track["name"]
            if all(ord(c) < 128 for c in title):  
                results.append({
                    "track_id": track["id"],
                    "title": title,
                    "artist": track["artists"][0]["name"],
                    "album": track["album"]["name"],
                    "release_date": track["album"]["release_date"],
                    "preview_url": track["preview_url"],
                    "ingest_ts": datetime.utcnow().isoformat()
                })
    return results

