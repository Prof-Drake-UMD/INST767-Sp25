import base64
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd
from google.cloud import pubsub_v1, storage

from schema_helpers import TRANSFORMERS
from dotenv import load_dotenv
load_dotenv()

# ── Environment setup ─────────────────────────────────────
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
CLEAN_BUCKET = os.getenv("CLEAN_BUCKET")
LOAD_TOPIC = os.getenv("LOAD_TOPIC")
RAW_BUCKET = os.getenv("RAW_BUCKET")

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()

logging.basicConfig(level=logging.INFO, format="[cleaner] %(message)s")

# ── Step 1: Download raw JSON ─────────────────────────────
def _read_raw(path: str) -> Any:
    if path.startswith("gs://"):
        # strip off the 'gs://', then split
        bucket, blob_path = path[5:].split("/", 1)
    else:
        bucket    = RAW_BUCKET
        blob_path = path

    blob = storage_client.bucket(bucket).blob(blob_path)
    data = blob.download_as_bytes()
    return json.loads(data)

# ── Step 2: Save as json ───────────────────────────────
# ── Step 2: Save as JSON ───────────────────────────────
def _write_json(df: pd.DataFrame, event_type: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_name = f"clean/{event_type}/{ts}.json"
    bucket = storage_client.bucket(CLEAN_BUCKET)
    blob = bucket.blob(blob_name)

    with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", delete=False) as tmp:
        df.to_json(tmp.name, orient='records', lines=True)
        blob.upload_from_filename(tmp.name)

    uri = f"gs://{CLEAN_BUCKET}/{blob_name}"
    logging.info("Wrote cleaned data → %s", uri)
    return uri



# ── Step 3: Publish cleaned path to loader ────────────────
def _publish_to_loader(uri: str, event_type: str) -> None:
    topic_path = publisher.topic_path(PROJECT_ID, LOAD_TOPIC)
    publisher.publish(
        topic_path,
        json.dumps({"gcs_uri": uri}).encode(),
        event_type=event_type,
    ).result()
    logging.info("Published to loader: %s", uri)

# ── Step 4: Main Cloud Function entry ─────────────────────
def handle_raw_message(event, context):
    logging.info("Received Pub/Sub event")
    try:
        payload: Dict = json.loads(base64.b64decode(event["data"]).decode())
        raw_path: str = payload.get("path")
        event_type: str = event.get("attributes", {}).get("event_type")
        if not raw_path or not event_type:
            logging.error("Missing required fields in Pub/Sub message: %s", payload)
            return  

        logging.info(f"Processing {event_type} from {raw_path}")
    except Exception as e:
        logging.error("Invalid Pub/Sub message: %s", e)
        return  

    try:
        raw_data = _read_raw(raw_path)
        df = TRANSFORMERS[event_type](raw_data)
        if df.empty:
            logging.warning(f"{event_type}: No rows after transform")
            return  
        json_uri = _write_json(df, event_type)
        logging.info(f"Cleaned {event_type} data saved to: {json_uri}")
        _publish_to_loader(json_uri, event_type)
        logging.info(f"Cleaner wrote and published: {json_uri}")

    except Exception as e:
        logging.exception("Cleaner failed: %s", e)
        raise  

