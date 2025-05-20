import requests
from datetime import datetime

# BOOKS: OpenLibrary
def fetch_books(title="Normal People", author="Sally Rooney"):
    url = f"https://openlibrary.org/search.json?q={title} {author}"
    resp = requests.get(url)
    books = []
    if resp.status_code == 200:
        docs = resp.json().get("docs", [])
        for book in docs:
            if "eng" in book.get("language", []) and book.get("first_publish_year", 0) >= 1950:
                books.append({
                    "book_id": book.get("key"),
                    "title": book.get("title"),
                    "author_name": book.get("author_name", [""])[0],
                    "first_publish_year": book.get("first_publish_year"),
                    "language": "eng",
                    "book_url": f"https://openlibrary.org{book.get('key')}",
                    "ingest_ts": datetime.utcnow().isoformat()
                })
    return books

# ARTWORKS: The Met
def fetch_artworks(year, buffer=10):
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

# MUSIC: Spotify
def get_spotify_token(client_id, client_secret):
    auth_url = "https://accounts.spotify.com/api/token"
    data = {"grant_type": "client_credentials"}
    resp = requests.post(auth_url, data=data, auth=(client_id, client_secret), timeout=10)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_music(year, token, limit=5):
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