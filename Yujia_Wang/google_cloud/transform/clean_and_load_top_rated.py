import json
import os
from datetime import datetime
from google.cloud import storage, bigquery
from clean_transform_movies import clean_movie
from schema import TOP_RATED_SCHEMA

# === Load raw top rated data ===
with open("top_rated_movies.json", "r", encoding="utf-8") as f:
    raw_movies = json.load(f)

# === Clean data ===
cleaned_movies = [clean_movie(movie, is_now_playing=False) for movie in raw_movies]

# === Save NDJSON locally ===
ndjson_path = "cleaned_top_rated.json"
with open(ndjson_path, "w", encoding="utf-8") as f:
    for movie in cleaned_movies:
        f.write(json.dumps(movie) + "\n")

print(f"✅ NDJSON saved locally as {ndjson_path}")

# === Upload to GCS ===
bucket_name = "movie-data-bucket-movie-pipeline-project"
blob_path = "cleaned/cleaned_top_rated.json"

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_path)

with open(ndjson_path, "rb") as f:
    blob.upload_from_file(f, content_type="application/json")

print(f"✅ Uploaded to GCS as gs://{bucket_name}/{blob_path}")

# === Load into BigQuery ===
bq_client = bigquery.Client()
table_ref = bq_client.dataset("movies_dataset").table("top_rated_movies")

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=TOP_RATED_SCHEMA,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
)

uri = f"gs://{bucket_name}/{blob_path}"
load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
print(f"📥 Starting BigQuery job: {load_job.job_id}")
load_job.result()
print("✅ Top rated movies loaded into BigQuery.")
