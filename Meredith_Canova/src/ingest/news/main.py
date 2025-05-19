from __future__ import annotations

import os
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict
from dotenv import load_dotenv
load_dotenv()

import requests

# Google Cloud clients
try:
    from google.cloud import storage, pubsub_v1
    _storage_client = storage.Client()
except ModuleNotFoundError:
    _storage_client = None

# ── Configuration ──────────────────────────────────────────
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
RAW_BUCKET = os.getenv("RAW_BUCKET")
RAW_DIR = Path("/tmp")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC = "market-transform"

# Pub/Sub publisher
_publisher = pubsub_v1.PublisherClient() if PROJECT_ID else None

# ── Companies to Track ─────────────────────────────────────
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

# ── Logging Setup ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="[news] %(message)s")

# ── Pub/Sub Message Publisher ──────────────────────────────
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

# ── NewsAPI Request Helper ─────────────────────────────────
def _call_newsapi(params: dict) -> dict:
    for attempt in range(3):
        try:
            r = requests.get("https://newsapi.org/v2/everything", params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            logging.warning("NewsAPI attempt %d failed: %s", attempt + 1, exc)
            time.sleep(2 * (attempt + 1))
    return {}

# ── Per-Company News Fetch ─────────────────────────────────
def _fetch_company_news(symbol: str, name: str) -> List[dict]:
    now = datetime.now(timezone.utc)
    params = {
        "q": name,
        "from": (now - timedelta(days=1)).date().isoformat(), 
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 100,
        "apiKey": NEWS_API_KEY,
    }

    payload = _call_newsapi(params)

    if payload.get("status") != "ok":
        logging.warning(f"[{symbol}] NewsAPI error: {payload.get('message')}")
        return []

    articles = payload.get("articles", [])
    logging.info(f"[{symbol}] Found {len(articles)} articles")

    if articles:
        logging.info(f"{symbol} sample article:\n{json.dumps(articles[0], indent=2)}")
    else:
        logging.debug(f"{symbol} full payload:\n{json.dumps(payload, indent=2)}")


    return [
        {
            "company": name,
            "ticker": symbol,
            "title": a.get("title"),
            "source": a.get("source", {}).get("name"),
            "published_at": a.get("publishedAt"),
            "url": a.get("url"),
        }
        for a in articles
    ]

# ── Save Raw JSON to GCS ───────────────────────────────────
def _save_raw(symbol: str, docs: List[dict]) -> str:
    fname = f"raw_news_{symbol}_{int(time.time())}.json"

    if _storage_client and RAW_BUCKET:
        blob = _storage_client.bucket(RAW_BUCKET).blob(f"raw/news/{fname}")
        blob.upload_from_string(json.dumps(docs))
        return f"gs://{RAW_BUCKET}/raw/news/{fname}"

    RAW_DIR.mkdir(exist_ok=True)
    path = RAW_DIR / fname
    with path.open("w") as f:
        json.dump(docs, f)
    return str(path)

# ── Cloud Function Entry Point ─────────────────────────────
def ingest_news(request=None):
    saved_paths = []

    if not NEWS_API_KEY:
        logging.error("Missing NEWS_API_KEY environment variable.")
        return "Error: NEWS_API_KEY is not set", 500

    for sym, name in COMPANIES.items():
        articles = _fetch_company_news(sym, name)
        if not articles:
            logging.warning(f"[{sym}] Skipped — no articles returned.")
            continue
            

        uri = _save_raw(sym, articles)
        saved_paths.append(uri)

        publish_message(
            TOPIC,
            {"path": uri, "symbol": sym, "company": name},
            event_type="news"
        )

        time.sleep(2)  

    msg = f"News: wrote {len(saved_paths)} files"
    logging.info(msg)
    return msg
if __name__ == "__main__":
    ingest_news()
