import os
import requests
import logging
import urllib.parse
from datetime import datetime
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"
TABLE = "books"

def get_book_data(title="Normal People", author="Sally Rooney"):
    """Retrieves book data from the Open Library API."""
    base_url = "https://openlibrary.org/search.json"

    
    title_encoded = urllib.parse.quote_plus(title)
    author_encoded = urllib.parse.quote_plus(author)

    url = f"{base_url}?q={title_encoded} {author_encoded}"

    logging.info(f"Fetching book data from: {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  
        data = response.json()
        docs = data.get("docs", [])
        results = []  

        for book in docs:
            if (
                "eng" in book.get("language", [])
                and book.get("first_publish_year", 0) >= 1950
            ):
                book_data = {
                    "book_id": book.get("key"),
                    "title": book.get("title"),
                    "author_name": book.get("author_name", [""])[0],
                    "first_publish_year": book.get("first_publish_year"),
                    "language": "eng",
                    "book_url": f"https://openlibrary.org{book.get('key')}",
                    "ingest_ts": datetime.utcnow().isoformat(),
                }
                results.append(book_data)

        logging.info(f"Found {len(results)} books matching the criteria.")
        return results

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from Open Library API: {e}")
        return []  

def write_to_bigquery(rows):
    """Writes book data to BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    logging.info(f"Writing {len(rows)} rows to BigQuery table {table_id}...")

    try:
        errors = client.insert_rows_json(table_id, rows)
        if errors:
            logging.error(f"BigQuery insert errors: {errors}")
            raise Exception(f"BigQuery insert errors: {errors}")  
        logging.info("Successfully wrote data to BigQuery.")

    except Exception as e:
        logging.error(f"Error writing to BigQuery: {e}")
        raise  

def main(event, context):
    """Main Cloud Function entry point."""
    logging.info("Starting book ingest Cloud Function...")
    try:
        books = get_book_data()
        if books:
            write_to_bigquery(books)
        else:
            logging.info("No books found matching the criteria.")

        logging.info("Book ingest Cloud Function completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during book ingest: {e}")
        raise
