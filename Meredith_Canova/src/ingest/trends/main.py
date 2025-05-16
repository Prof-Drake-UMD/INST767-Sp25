from __future__ import annotations

import os
import time
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
import requests

# Google Cloud clients
try:
    from google.cloud import storage, pubsub_v1
    _storage_client = storage.Client()
except ModuleNotFoundError:
    _storage_client = None

# ── Config ─────────────────────────────────────────────────
SEARCH_API_KEY = os.getenv("SEARCH_API_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET")
RAW_DIR = Path("/tmp")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC = "market-transform"

_publisher = pubsub_v1.PublisherClient() if PROJECT_ID else None

logging.basicConfig(level=logging.INFO, format="[trends] %(message)s")

# ── Pub/Sub Publisher ──────────────────────────────────────
def publish_message(topic_id: str, payload: dict, **attributes: str) -> None:
    if not _publisher:
        logging.warning("Skipped publish – GCP_PROJECT_ID not set.")
        return

    try:
        topic_path = _publisher.topic_path(PROJECT_ID, topic_id)
        future = _publisher.publish(
            topic_path,
            data=json.dumps(payload).encode("utf-8"),
            **{k: str(v) for k, v in attributes.items()}
        )
        ack = future.result()
        logging.info(f"Published message to {topic_id} (ack: {ack})")
    except Exception as e:
        logging.error(f"Failed to publish to {topic_id}: {e}")

# ── API Request ────────────────────────────────────────────
def _call_searchapi() -> list[dict]:
    params = {
        "engine": "google_trends_trending_now",
        "geo": "US",
        "api_key": SEARCH_API_KEY,
    }

    try:
        r = requests.get("https://www.searchapi.io/api/v1/search", params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        trends = data.get("trends")
        if not trends or not isinstance(trends, list):
            logging.warning("No trends found in SearchAPI response.")
            return []
        
        # Normalize records
        records = []
        for item in trends:
            records.append({
                "position": item.get("position"),
                "query": item.get("query"),
                "search_volume": item.get("search_volume"),
                "percentage_increase": item.get("percentage_increase"),
                "location": item.get("location"),
                "categories": item.get("categories"),
                "start_date": item.get("start_date"),
                "end_date": item.get("end_date"),
                "time_active_minutes": item.get("time_active_minutes"),
                "keywords": item.get("keywords")
            })
        return records
    except Exception as e:
        logging.error(f"SearchAPI request failed: {e}")
        return []


# ── Save to Cloud Storage ─────────────────────────────────
def _save_raw(payload: dict) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fname = f"raw_trends_{ts}.json"

    if _storage_client and RAW_BUCKET:
        blob = _storage_client.bucket(RAW_BUCKET).blob(f"raw/trends/{fname}")
        blob.upload_from_string(json.dumps(payload))
        return f"gs://{RAW_BUCKET}/raw/trends/{fname}"

    RAW_DIR.mkdir(exist_ok=True)
    path = RAW_DIR / fname
    with path.open("w") as f:
        json.dump(payload, f)
    return str(path)

# ── Main Logic ─────────────────────────────────────────────
def main():
    if not SEARCH_API_KEY:
        logging.error("Missing SEARCH_API_KEY.")
        return "Error: SEARCH_API_KEY is not set", 500
    
    records = _call_searchapi()
    if not records:
        return "Error: No data returned from SearchAPI", 500
    payload = {"trends": records}
    uri = _save_raw(payload)

    publish_message(TOPIC, {"path": uri}, event_type="trends")

    msg = f" trends: wrote {uri}"
    logging.info(msg)
    return msg

# ── Cloud Function Entry ───────────────────────────────────
def ingest_trends(request):
    return main()
if __name__ == "__main__":
    main()
