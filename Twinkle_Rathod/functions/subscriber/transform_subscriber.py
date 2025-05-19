import json
import pandas as pd
import logging  #for pipeline health
from google.cloud import storage, bigquery
from transform import transform_dc_data, transform_nyc_data, transform_cdc_data
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "gv-etl-spring"
BUCKET_NAME = "gv-etl-bucket"
DATASET_ID = "gun_violence_dataset"
TABLE_ID = "firearm_incidents"


def download_json_from_gcs(gcs_path):
    logger.info(f"Downloading from GCS: {gcs_path}")
    bucket_name, blob_path = gcs_path.replace("gs://", "").split("/", 1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    return json.loads(content)


def transform_from_pubsub(event, context):
    logger.info("Pub/Sub event triggered")

    try:
        payload = base64.b64decode(event['data']).decode("utf-8")
        message = json.loads(payload)
        source = message.get("source")
        gcs_path = message.get("gcs_path")

        logger.info(f"Source: {source}, File: {gcs_path}")

        if not source or not gcs_path:
            raise ValueError("Missing source or gcs_path in message")

        raw_data = download_json_from_gcs(gcs_path)

        if source == "dc":
            df = transform_dc_data(raw_data)
        elif source == "nyc":
            df = transform_nyc_data(raw_data)
        elif source == "cdc":
            df = transform_cdc_data(raw_data)
        else:
            raise ValueError(f"Unknown source: {source}")

        logger.info(f"Transformation complete. Rows: {len(df)}")
        
        #loading to BQ
        bq_client = bigquery.Client()
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        load_job = bq_client.load_table_from_dataframe(df, table_ref)
        load_job.result()
        logger.info(f"Loaded {len(df)} rows from {source} into BigQuery.")
        
    except Exception as e:
        logger.error("Error in transform_from_pubsub", exc_info=True)


def base64_decode(data):
    import base64
    return base64.b64decode(data).decode("utf-8")
