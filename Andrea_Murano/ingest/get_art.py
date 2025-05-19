import requests
import random
from datetime import datetime

def get_artworks_near_year(year, buffer=10, max_results=5):
    all_results = []
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"

    for yr in range(year - buffer, year + buffer + 1):
        params = {"q": str(yr), "hasImages": True}
        resp = requests.get(search_url, params=params)
        ids = resp.json().get("objectIDs", []) or []
        random.shuffle(ids)
        for object_id in ids[:max_results]:
            obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{object_id}"
            data = requests.get(obj_url).json()
            if data:
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
    return all_results
