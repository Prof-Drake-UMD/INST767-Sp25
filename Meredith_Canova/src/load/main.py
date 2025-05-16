from __future__ import annotations
import base64
import json
import logging
import os
import pathlib
from dotenv import load_dotenv

from typing import Tuple
import pandas as pd
from google.cloud import bigquery, pubsub_v1

from bq_schema import schema_map

logging.basicConfig(level=logging.INFO, format="[loader] %(message)s")

# ── Load environment ─────────────────────────────────────────────
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET")
SUB_ID = os.getenv("LOAD_SUB")  # pull-mode only

bq = bigquery.Client(project=PROJECT_ID)
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUB_ID)

# ── Map event_type → (table_name, schema) ─────────────────────────
TABLE_MAP: dict[str, Tuple[str, list[bigquery.SchemaField]]] = {
    "stocks": ("stocks_daily",   schema_map["stocks"]),
    "news":   ("company_news",   schema_map["news"]),
    "trends": ("google_trends",  schema_map["trends"]),
}

# ── Ensure dataset & table exist ─────────────────────────────────
def _ensure_dataset_and_table(event_type: str) -> None:
    ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    bq.create_dataset(ds_ref, exists_ok=True)
    table_name, schema = TABLE_MAP[event_type]
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    try:
        bq.get_table(table_id)
    except Exception:
        tbl = bigquery.Table(table_id, schema=schema)
        bq.create_table(tbl)
        logging.info("Created table %s", table_id)

# ── Load a single CSV from GCS or local into BigQuery ────────────
def _load_json_to_bq(gcs_uri: str, table_name: str, schema):
    job_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    bq.load_table_from_uri(gcs_uri, table_id, job_config=job_cfg).result()
    logging.info("Loaded %s → %s", gcs_uri, table_id)



# ── Core processing ──────────────────────────────────────────────
def _process(event_type: str, gcs_uri: str):
    if event_type not in TABLE_MAP:
        raise ValueError(f"Unknown event_type '{event_type}'")
    _ensure_dataset_and_table(event_type)
    table, schema = TABLE_MAP[event_type]
    _load_json_to_bq(gcs_uri, table, schema)


# ── Entry point for Pub/Sub-triggered loader function ────────────
def handle_json_message(event, context):
    """Cloud Function: triggered by Pub/Sub when cleaner publishes."""
    try:
        attrs = event["attributes"]
        event_type = attrs["event_type"]
        gcs_uri = json.loads(base64.b64decode(event["data"]).decode())["gcs_uri"]
        _process(event_type, gcs_uri)
    except Exception:
        logging.exception("Failed to handle message")
        raise

# ── Pull-subscription callback (for local testing only) ──────────
def _callback(msg: pubsub_v1.subscriber.message.Message):
    try:
        event_type = msg.attributes["event_type"]
        gcs_uri = json.loads(msg.data)["gcs_uri"]
        _process(event_type, gcs_uri)
        msg.ack()
    except Exception:
        logging.exception("Error processing message – NACK")
        msg.nack()

# ── CLI helper to load all local CSVs ─────────────────────────────
def load_local(clean_dir: str):
    """Load every .csv in `clean_dir` from your file system."""
    for path in pathlib.Path(clean_dir).glob("*.json"):
        # infer event_type from filename
        event_type = (
            "stocks" if "stocks" in path.name else
            "news"   if "news" in path.name else
            "trends"
        )
        gcs_uri = f"file://{path.resolve()}"
        logging.info(f"[local] Loading {gcs_uri} as {event_type}")
        _process(event_type, gcs_uri)

# ── CLI dispatch ──────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="[loader] %(message)s")
    if len(sys.argv) == 2 and sys.argv[1] == "subscribe":
        logging.info("[pull] Subscribing to %s", subscription_path)
        subscriber.subscribe(subscription_path, callback=_callback).result()
    elif len(sys.argv) == 2:
        load_local(sys.argv[1])
    else:
        print("Usage:")
        print("  python -m src.load.main subscribe")
        print("  python -m src.load.main <local_clean_dir>")