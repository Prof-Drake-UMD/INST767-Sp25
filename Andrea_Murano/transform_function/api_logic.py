import os
import requests
import time

SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")

def get_spotify_token():
    auth_url = "https://accounts.spotify.com/api/token"
    data = {"grant_type": "client_credentials"}
    response = requests.post(auth_url, data=data, auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET))
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception("Failed to get Spotify token: " + response.text)

def get_book_by_keyword(book_title, author_name):
    url = "https://openlibrary.org/search.json"
    params = {"title": book_title, "author": author_name}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        docs = data.get("docs", [])
        if docs:
            doc = docs[0]  
            return {
                "book_id": doc.get("key"),
                "title": doc.get("title"),
                "author_name": doc.get("author_name", [None])[0],
                "first_publish_year": doc.get("first_publish_year"),
                "language": doc.get("language", [None])[0] if doc.get("language") else None,
                "book_url": f"https://openlibrary.org{doc.get('key')}" if doc.get("key") else None,
                "ingest_ts": int(time.time())
            }
    return {}

def get_met_artworks_by_year_range(year, buffer=10, limit=10):
    if not year:
        return []
    min_year = year - buffer
    max_year = year + buffer
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"
    params = {
        "q": "",
        "hasImages": "true",
        "dateBegin": min_year,
        "dateEnd": max_year
    }
    response = requests.get(search_url, params=params)
    if response.status_code != 200:
        return []
    data = response.json()
    objectIDs = data.get("objectIDs", [])[:limit]
    results = []
    for oid in objectIDs:
        obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{oid}"
        obj_resp = requests.get(obj_url)
        if obj_resp.status_code == 200:
            obj = obj_resp.json()
            results.append({
                "object_id": obj.get("objectID"),
                "title": obj.get("title"),
                "artist_name": obj.get("artistDisplayName"),
                "medium": obj.get("medium"),
                "object_date": obj.get("objectDate"),
                "object_url": obj.get("objectURL"),
                "image_url": obj.get("primaryImageSmall")
            })
    return results

def get_spotify_tracks_by_year(year, token=None, limit=10):
    if not year:
        return []
    if token is None:
        token = get_spotify_token()
    search_url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": f"year:{year}",
        "type": "track",
        "limit": 50
    }
    response = requests.get(search_url, headers=headers, params=params)
    if response.status_code != 200:
        return []
    tracks = response.json().get("tracks", {}).get("items", [])
    results = []
    latin_char_results = []
    for track in tracks:
        album = track.get("album", {})
        artists = track.get("artists", [])
        track_data = {
            "track_id": track.get("id"),
            "title": track.get("name"),
            "artist": artists[0]["name"] if artists else None,
            "album": album.get("name"),
            "release_date": album.get("release_date"),
            "preview_url": track.get("preview_url")
        }
        if all(ord(c) < 128 for c in track_data["title"] or ""):
            latin_char_results.append(track_data)
        else:
            results.append(track_data)
        if len(latin_char_results) + len(results) >= limit:
            break
    if len(latin_char_results) >= limit:
        return latin_char_results[:limit]
    return latin_char_results + results[:max(0, limit - len(latin_char_results))]

def match_cultural_experience_by_year(book_title, author_name, limit=5, year_buffer=10):
    book = get_book_by_keyword(book_title, author_name)
    if not book:
        return {"error": "Book not found."}
    year = book.get("first_publish_year")
    artworks = get_met_artworks_by_year_range(year, buffer=year_buffer, limit=limit)
    music = get_spotify_tracks_by_year(year, limit=limit * 2)  # token will be fetched as needed
    return {
        "step": "year_only",
        "book": book,
        "artworks": artworks[:limit],
        "music": music[:limit]
    }
