from __future__ import annotations

import os
import time
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
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
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET")
RAW_DIR = Path("/tmp")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC = "market-transform"

_publisher = pubsub_v1.PublisherClient() if PROJECT_ID else None

COMPANIES: Dict[str, str] = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "NVDA": "NVIDIA",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "META": "Meta Platforms",
    "JPM":  "JPMorgan Chase",
    "V":    "Visa",
    "MA":   "Mastercard",
    "BAC":  "Bank of America",
    "WFC":  "Wells Fargo",
    "C":    "Citigroup",
    "UNH":  "UnitedHealth Group",
    "JNJ":  "Johnson & Johnson",
    "PFE":  "Pfizer",
    "LLY":  "Eli Lilly",
    "MRK":  "Merck",
    "ABBV": "AbbVie",
    "PEP":  "PepsiCo",
    "KO":   "Coca-Cola",
    "MDLZ": "Mondelez International",
    "KHC":  "Kraft Heinz",
    "TSN":  "Tyson Foods",
    "SJM":  "J.M. Smucker",
}

logging.basicConfig(level=logging.INFO, format="[stocks] %(message)s")

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

# ── Polygon API Call ───────────────────────────────────────
def _call_polygon(symbol: str) -> dict:
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            logging.warning(f"[{symbol}] attempt {attempt+1} failed: {exc}")
            time.sleep(2 * (attempt + 1))
    return {}

# ── Save JSON to GCS or /tmp ───────────────────────────────
def _save_raw(symbol: str, payload: dict) -> str:
    ts = int(time.time())
    fname = f"raw_stocks_{symbol}_{ts}.json"

    if _storage_client and RAW_BUCKET:
        blob = _storage_client.bucket(RAW_BUCKET).blob(f"raw/stocks/{fname}")
        blob.upload_from_string(json.dumps(payload))
        return f"gs://{RAW_BUCKET}/raw/stocks/{fname}"

    RAW_DIR.mkdir(exist_ok=True)
    path = RAW_DIR / fname
    with path.open("w") as f:
        json.dump(payload, f)
    return str(path)

# ── Main Logic ─────────────────────────────────────────────
def main():
    saved = []

    if not POLYGON_API_KEY:
        logging.error("Missing POLYGON_API_KEY.")
        return "Error: POLYGON_API_KEY is not set", 500

    for sym in COMPANIES:
        data = _call_polygon(sym)
        if not data:
            continue

        uri = _save_raw(sym, data)
        saved.append(uri)

        publish_message(TOPIC, {"path": uri, "symbol": sym}, event_type="stocks")

        time.sleep(13)  # API rate limit protection

    msg = f"stocks: wrote {len(saved)} files"
    logging.info(msg)
    return msg

# ── Cloud Function Entry ───────────────────────────────────
def ingest_stocks(request):
    return main()
if __name__ == "__main__":
    main()
