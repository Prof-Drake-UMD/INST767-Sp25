# google_cloud/load_to_bigquery.py

from google.cloud import bigquery

import os,sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(PARENT_DIR)

from schema import NOW_PLAYING_SCHEMA
def load_json_to_bigquery(bucket_name, blob_path, dataset_id, table_id):
    client = bigquery.Client()
    uri = f"gs://{bucket_name}/{blob_path}"

    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=NOW_PLAYING_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

    print(f"📥 Starting job: {load_job.job_id}")
    load_job.result()
    print("✅ Data loaded into BigQuery table.")

if __name__ == "__main__":
    BUCKET_NAME = "movie-data-bucket-movie-pipeline-project"
    FILE_PATH = "cleaned/cleaned.json"  
    DATASET_ID = "movies_dataset"
    TABLE_ID = "now_playing_movies"

    load_json_to_bigquery(BUCKET_NAME, FILE_PATH, DATASET_ID, TABLE_ID)
