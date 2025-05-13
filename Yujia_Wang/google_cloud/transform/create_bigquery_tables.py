from dotenv import load_dotenv
from google.cloud import bigquery
import os,sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(PARENT_DIR)

from schema import NOW_PLAYING_SCHEMA, TOP_RATED_SCHEMA


# Load the .env file one level up from the current script
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Now get the PROJECT_ID
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "movies_dataset"


def create_dataset_if_not_exists(client):
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset {DATASET_ID} already exists.")
    except Exception:
        client.create_dataset(dataset_ref)
        print(f"📁 Created dataset: {DATASET_ID}")

def create_table_if_not_exists(client, table_id, schema):
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    table_ref = bigquery.Table(full_table_id, schema=schema)
    try:
        client.get_table(full_table_id)
        print(f"✅ Table {table_id} already exists.")
    except Exception:
        client.create_table(table_ref)
        print(f"📄 Created table: {table_id}")

if __name__ == "__main__":
    client = bigquery.Client(project=PROJECT_ID)
    create_dataset_if_not_exists(client)
    create_table_if_not_exists(client, "now_playing_movies", NOW_PLAYING_SCHEMA)
    create_table_if_not_exists(client, "top_rated_movies", TOP_RATED_SCHEMA)
