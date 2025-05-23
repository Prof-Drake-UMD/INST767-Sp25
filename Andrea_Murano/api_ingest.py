import requests
from datetime import datetime
import json

SPOTIFY_CLIENT_ID = "653aec7ed52e453a9db9eb5796a0f306"
SPOTIFY_CLIENT_SECRET = "68983ccf13a7463f91f7c636f04cff21"


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


def fetch_artwork():
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"
    object_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects/"
    results = []
    resp = requests.get(search_url, params={"q": "", "hasImages": True}, timeout=10)
    if resp.status_code == 200:
        ids = resp.json().get("objectIDs", []) or []
        for object_id in ids[:5]:  # limit for example
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
    params = {
        "q": "year:>=1950",
        "type": "track",
        "limit": 5  # limit for example
    }
    resp = requests.get(search_url, headers=headers, params=params, timeout=10)
    if resp.status_code == 200:
        for track in resp.json().get("tracks", {}).get("items", []):
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
    return results

if __name__ == "__main__":
    books = fetch_books()
    with open("books.json", "w", encoding="utf-8") as f:
        json.dump(books, f, ensure_ascii=False, indent=2)
    print("Wrote books.json")

    artworks = fetch_artwork()
    with open("artworks.json", "w", encoding="utf-8") as f:
        json.dump(artworks, f, ensure_ascii=False, indent=2)
    print("Wrote artworks.json")

    token = get_spotify_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)
    music = fetch_music(token)
    with open("music.json", "w", encoding="utf-8") as f:
        json.dump(music, f, ensure_ascii=False, indent=2)
    print("Wrote music.json")
