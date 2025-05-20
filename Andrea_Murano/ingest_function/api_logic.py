import os
import requests
import time
import random
import re
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
GCP_PROJECT = os.getenv("GCP_PROJECT")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC")


def get_spotify_token():
    auth_url = "https://accounts.spotify.com/api/token"
    data = {"grant_type": "client_credentials"}
    response = requests.post(auth_url, data=data, auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET), timeout=10)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception("Failed to get Spotify token: " + response.text)

def get_book_by_keyword(book_title, author_name):
    query = f"{book_title} {author_name}".strip()
    url = f"https://openlibrary.org/search.json?q={query}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            docs = response.json().get("docs", [])
            for book in docs[:15]:
                book_title_l = book.get("title", "").lower()
                authors = [a.lower() for a in book.get("author_name", [])]
                publish_year = book.get("first_publish_year")
                work_key = book.get("key")
                languages = book.get("language", [])
 
                if (
                    book_title.lower() in book_title_l and
                    any(author_name.lower() in a for a in authors) and
                    "eng" in languages and
                    publish_year and publish_year >= 1950 and
                    work_key
                ):
                    return {
                        "book_id": work_key,
                        "title": book.get("title"),
                        "author_name": book.get("author_name", [None])[0],
                        "first_publish_year": publish_year,
                        "language": "eng",
                        "book_url": f"https://openlibrary.org{work_key}",
                        "ingest_ts": datetime.utcnow().isoformat()
                    }
    except Exception as e:
        print(f"Book API exception: {e}")
    return {}

def get_met_artworks_by_year_range(year, buffer=10, limit=10):
    if not year:
        return []
    all_results = []
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"
    seen_ids = set()
    for yr in range(year-buffer, year+buffer+1):
        try:
            resp = requests.get(search_url, params={"q": str(yr), "hasImages": True}, timeout=10)
            if resp.status_code != 200:
                continue
            ids = resp.json().get("objectIDs", []) or []
            sampled_ids = random.sample(ids, min(5, len(ids)))
            for object_id in sampled_ids:
                if object_id in seen_ids:
                    continue
                obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{object_id}"
                obj_resp = requests.get(obj_url, timeout=10)
                if obj_resp.status_code == 200:
                    data = obj_resp.json()
                    obj_year = data.get("objectBeginDate", 0)
                    if data and 1950 <= obj_year <= 2025 and abs(obj_year - year) <= buffer:
                        all_results.append({
                            "object_id": data.get("objectID"),
                            "title": data.get("title"),
                            "artist_name": data.get("artistDisplayName"),
                            "object_date": data.get("objectDate"),
                            "medium": data.get("medium"),
                            "image_url": data.get("primaryImageSmall"),
                            "object_url": data.get("objectURL"),
                            "ingest_ts": datetime.utcnow().isoformat()
                        })
                        seen_ids.add(object_id)
        except Exception as e:
            print(f"Art API exception for year {yr}: {e}")

    return all_results[:limit]

def get_spotify_tracks_by_year(year, token=None, limit=10):
    if not year:
        return []
    if token is None:
        try:
            token = get_spotify_token()
        except Exception as e:
            print(f"Spotify token error: {e}")
            return []
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    results = []
    latin_char_results = []
    for attempt in range(3):
        params = {
            "q": f"year:{year}",
            "type": "track",
            "limit": 50
        }
        try:
            resp = requests.get(search_url, headers=headers, params=params, timeout=10)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 1))
                print(f"Rate limited by Spotify. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue
            elif resp.status_code != 200:
                print(f"Spotify API error: {resp.status_code}")
                break
            data = resp.json().get("tracks", {}).get("items", [])
            for track in data:
                track_data = {
                    "track_id": track.get("id"),
                    "title": track.get("name"),
                    "artist": track.get("artists", [{}])[0].get("name", ""),
                    "album": track.get("album", {}).get("name", ""),
                    "release_date": track.get("album", {}).get("release_date", ""),
                    "preview_url": track.get("preview_url"),
                    "ingest_ts": datetime.utcnow().isoformat()
                }

                if re.match(r'^[A-Za-z0-9\s\.,!?\'"()\-\[\]:;]+$', track.get("name", "")):
                    latin_char_results.append(track_data)
                else:
                    results.append(track_data)
            break
        except Exception as e:
            print(f"Music API exception: {e}")
    if len(latin_char_results) >= limit:
        return latin_char_results[:limit]
    else:
        return latin_char_results + results[:max(0, limit - len(latin_char_results))]

def match_cultural_experience_by_year(book_title, author_name, limit=5, year_buffer=10):
    book = get_book_by_keyword(book_title, author_name)
    if not book:
        return {"error": "Book not found."}
    year = book.get("first_publish_year")
    artworks = get_met_artworks_by_year_range(year, buffer=year_buffer, limit=limit)
    music = get_spotify_tracks_by_year(year, limit=limit * 2)
    return {
        "step": "year_only",
        "book": book,
        "artworks": artworks[:limit],
        "music": music[:limit]
    }
