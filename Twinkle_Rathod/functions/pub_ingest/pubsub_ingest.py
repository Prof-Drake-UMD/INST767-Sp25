import requests
import json
import logging  #for pipeline health
from datetime import datetime
from google.cloud import pubsub_v1, storage
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "gv-etl-spring"
BUCKET_NAME = "gv-etl-bucket"
TOPIC_ID = "gun-violence-ingest"

publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def publish_to_pubsub(message: dict):
    message_bytes = json.dumps(message).encode("utf-8")
    future = publisher.publish(topic_path, message_bytes)
    print(f"Published to Pub/Sub: {message}")


def upload_to_gcs(data, filename):
    local_path = f"/tmp/{filename}"
    with open(local_path, "w") as f:
        json.dump(data, f)

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"ingest/{filename}")
    blob.upload_from_filename(local_path)
    gcs_path = f"gs://{BUCKET_NAME}/ingest/{filename}"
    print(f"Uploaded to GCS: {gcs_path}")
    return gcs_path


def fetch_dc_data():
    logger.info("Fetching DC data...")
    url = "https://opendata.arcgis.com/datasets/89bfd2aed9a142249225a638448a5276_29.geojson"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        gcs_path = upload_to_gcs(data, f"dc_data_{datetime.today().date()}.geojson")
        publish_to_pubsub({"source": "dc", "gcs_path": gcs_path})
        logger.info("DC data published to Pub/Sub.")
    except Exception as e:
        logger.error(f"Failed to fetch or publish DC data: {e}")


def fetch_nyc_data(limit=1000):
    logger.info("Fetching NYC data...")
    url = "https://data.cityofnewyork.us/resource/5uac-w243.json"
    params = {"$where": "pd_desc LIKE '%FIREARM%'", "$limit": limit}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        filename = f"nyc_data_{datetime.today().date()}.json"
        gcs_path = upload_to_gcs(data, filename)
        publish_to_pubsub({"source": "nyc", "gcs_path": gcs_path})
        logger.info("NYC data published to Pub/Sub.")
    except Exception as e:
        logger.error(f"Failed to fetch or publish NYC data: {e}")



def fetch_cdc_data(limit=1000):
    logger.info("Fetching CDC data...")
    url = "https://data.cdc.gov/resource/fpsi-y8tj.json"
    params = {"$limit": limit, "$where": "intent LIKE 'FA_%'"}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        gcs_path = upload_to_gcs(data, f"cdc_data_{datetime.today().date()}.json")
        publish_to_pubsub({"source": "cdc", "gcs_path": gcs_path})
        logger.info("CDC data published to Pub/Sub.")
    except Exception as e:
        logger.error(f"Failed to fetch or publish CDC data: {e}")
    


def run_ingest():
    logger.info("Starting Pub/Sub ingestion")
    fetch_dc_data()
    fetch_nyc_data()
    fetch_cdc_data()
    logger.info("Ingest complete")
