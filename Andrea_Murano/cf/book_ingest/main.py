import os
import requests
from datetime import datetime
from google.cloud import bigquery

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "books"

def get_book_data(title="Normal People", author="Sally Rooney"):
    url = f"https://openlibrary.org/search.json?q={title} {author}"
    response = requests.get(url)
    if response.status_code == 200:
        docs = response.json().get("docs", [])
        for book in docs:
            if "eng" in book.get("language", []) and book.get("first_publish_year", 0) >= 1950:
                return [{
                    "book_id": book.get("key"),
                    "title": book.get("title"),
                    "author_name": book.get("author_name", [""])[0],
                    "first_publish_year": book.get("first_publish_year"),
                    "language": "eng",
                    "book_url": f"https://openlibrary.org{book.get('key')}",
                    "ingest_ts": datetime.utcnow().isoformat()
                }]
    return []

def write_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("BigQuery error:", errors)

def main(event, context):
    books = get_book_data()
    if books:
        write_to_bigquery(books)
