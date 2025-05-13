from google.cloud import bigquery
import json
import os,sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(PARENT_DIR)
from schema import TOP_RATED_SCHEMA
def load_local_json_to_bigquery(json_path, dataset_id, table_id):
    client = bigquery.Client()

    with open(json_path, "r") as f:
        records = json.load(f)

    table_ref = client.dataset(dataset_id).table(table_id)

    errors = client.insert_rows_json(table=table_ref, json_rows=records)

    if errors:
        print("❌ Failed to insert rows:")
        for err in errors:
            print(err)
    else:
        print(f"✅ Inserted {len(records)} rows into {dataset_id}.{table_id}")

if __name__ == "__main__":
    PROJECT_ID = os.getenv("PROJECT_ID")
    json_path = "top_rated_movies.json" 
    dataset_id = "movies_dataset"
    table_id = "top_rated_movies"

    load_local_json_to_bigquery(json_path, dataset_id, table_id)
