import requests
import random
from datetime import datetime

def get_artworks_near_year(year, buffer=10, max_results=5):
    if not isinstance(year, int):
        raise ValueError("Year must be integer")
    all_results = []
    search_url = "https://collectionapi.metmuseum.org/public/collection/v1/search"
    seen_ids = set()
    for yr in range(year-buffer, year+buffer+1):
        try:
            resp = requests.get(search_url, params={"q": str(yr), "hasImages": True}, timeout=10)
            if resp.status_code != 200:
                continue
            ids = resp.json().get("objectIDs", []) or []
            sampled_ids = random.sample(ids, min(max_results, len(ids)))
            for object_id in sampled_ids:
                if object_id in seen_ids:
                    continue
                obj_url = f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{object_id}"
                data = requests.get(obj_url, timeout=10).json()
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
    return all_results
