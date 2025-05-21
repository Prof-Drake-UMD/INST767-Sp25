import requests
from datetime import datetime

# BOOKS: OpenLibrary
def fetch_books():
    url = "https://openlibrary.org/search.json?q="
    resp = requests.get(url)
    books = []
    if resp.status_code == 200:
        docs = resp.json().get("docs", [])
        for book in docs:
            year = book.get("first_publish_year", 0)
            if year and year >= 1950:
                books.append({
                    "book_id": book.get("key"),
                    "title": book.get("title"),
                    "author_name": book.get("author_name", [""])[0],
                    "first_publish_year": year,
                    "language": book.get("language", [""])[0] if book.get("language") else "",
                    "book_url": f"https://openlibrary.org{book.get('key')}",
                    "ingest_ts": datetime.utcnow().isoformat()
                })
    return books

# ARTWORK: The Met
def fetch_artwork():
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"
    object_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/"
    results = []
    resp = requests.get(search_url, params={"q": "", "hasImages": True}, timeout=10)
    if resp.status_code == 200:
        ids = resp.json().get("objectIDs", []) or []
        for object_id in ids:
            obj_data = requests.get(object_url + str(object_id), timeout=10).json()
            if obj_data:
                end_date = obj_data.get("objectEndDate")
                try:
                    end_date_int = int(float(end_date))
                except (TypeError, ValueError):
                    end_date_int = None
                if end_date_int is not None and end_date_int >= 1950:
                    results.append({
                        "object_id": int(obj_data.get("objectID")),
                        "title": obj_data.get("title"),
                        "artist_name": obj_data.get("artistDisplayName"),
                        "medium": obj_data.get("medium"),
                        "object_date": obj_data.get("objectDate"),
                        "object_url": obj_data.get("objectURL"),
                        "image_url": obj_data.get("primaryImageSmall"),
                        "objectEndDate": end_date_int,
                        "ingest_ts": datetime.utcnow().isoformat()
                    })
    return results

# MUSIC: Spotify
def get_spotify_token(client_id, client_secret):
    auth_url = "https://accounts.spotify.com/api/token"
    data = {"grant_type": "client_credentials"}
    resp = requests.post(auth_url, data=data, auth=(client_id, client_secret), timeout=10)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_music(token):
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    results = []
    offset = 0
    limit = 50
    while True:
        params = {
            "q": "year:>=1950",
            "type": "track",
            "limit": limit,
            "offset": offset
        }
        resp = requests.get(search_url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            break
        items = resp.json().get("tracks", {}).get("items", [])
        if not items:
            break
        for track in items:
            release_date = track.get("album", {}).get("release_date", None)
            try:
                release_year = int(str(release_date)[:4])
            except (TypeError, ValueError):
                release_year = None
            if release_year is not None and release_year >= 1950:
                results.append({
                    "track_id": track.get("id"),
                    "title": track.get("name"),
                    "artist": track.get("artists", [{}])[0].get("name", ""),
                    "album": track.get("album", {}).get("name", ""),
                    "release_date": release_date,
                    "preview_url": track.get("preview_url"),
                    "ingest_ts": datetime.utcnow().isoformat()
                })
        if len(items) < limit:
            break
        offset += limit
    return results
