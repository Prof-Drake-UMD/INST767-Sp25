import os
import logging
from datetime import datetime, timezone
import functions_framework
from .base_handler import MessageHandler, BaseTransformer, BigQueryLoader

logger = logging.getLogger(__name__)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gcp-project-id")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET_NAME", "dataset")
NYT_TABLE_NAME = "nyt_books"
TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{NYT_TABLE_NAME}"

class NytBookTransformer(BaseTransformer):
    """Transforms NYT book data."""

    def _transform_item(self, book, index):
        """Transforms a single NYT book item."""
        isbn13 = book.get("primary_isbn13") or book.get("isbn13") or book.get("isbns", [{}])[0].get("isbn13")
        if not isbn13:
            logger.warning(f"[Event ID: {self.event_id}] Missing ISBN13 for NYT book at index {index}: {book.get('title', 'N/A')}. Skipping.")
            return None

        transformed_book = {
            "ingest_date": self.current_ingest_date,
            "rank": book.get("rank"),
            "rank_last_week": book.get("rank_last_week"),
            "weeks_on_list": book.get("weeks_on_list"),
            "publisher": book.get("publisher"),
            "description": book.get("description"),
            "title": book.get("title"),
            "author": book.get("author"),
            "amazon_product_url": book.get("amazon_product_url"),
            "isbn13": str(isbn13).replace("-", "").strip(),
            "isbn10": str(book.get("primary_isbn10", "")).replace("-", "").strip(),
            "bestseller_date": book.get("_bestseller_date"),
            "list_name": book.get("_list_name_from_api") or book.get("_list_name_encoded")
        }
        return transformed_book

@functions_framework.cloud_event
def process_nyt_data_subscriber(cloud_event):
    """Triggered by a Pub/Sub message to process NYT books data and load to BigQuery."""
    event_id = cloud_event.id if hasattr(cloud_event, 'id') else 'N/A'
    event_source = cloud_event.source if hasattr(cloud_event, 'source') else 'N/A'
    logger.info(f"[Event ID: {event_id}] Received NYT data event from {event_source}")

    message_handler = MessageHandler(event_id)
    parsed_message_payload = message_handler.decode_and_parse(cloud_event.data)

    if parsed_message_payload is None:
        return 
    
    if not isinstance(parsed_message_payload, list):
        logger.error(f"[Event ID: {event_id}] Expected parsed_message_payload to be a list. Got type: {type(parsed_message_payload)}. Data: {str(parsed_message_payload)[:200]}")
        return

    if not parsed_message_payload:
        logger.warning(f"[Event ID: {event_id}] Received an empty list in parsed_message_payload. Nothing to process.")
        return

    if len(parsed_message_payload) > 1:
        logger.warning(f"[Event ID: {event_id}] parsed_message_payload list contains {len(parsed_message_payload)} items. Processing only the first one for NYT.")
    
    actual_api_response_dict = parsed_message_payload[0]

    if not isinstance(actual_api_response_dict, dict):
        logger.error(f"[Event ID: {event_id}] Expected the first item in parsed_message_payload to be a dict. Got {type(actual_api_response_dict)}. Data: {str(actual_api_response_dict)[:200]}")
        return

    if isinstance(actual_api_response_dict.get("results"), dict):
        books_data = actual_api_response_dict.get("results", {}).get("books")
        if books_data is None: 
            logger.warning(f"[Event ID: {event_id}] 'books' array not found or is null in actual_api_response_dict.results. Response: {str(actual_api_response_dict)[:500]}")
            return
        logger.info(f"[Event ID: {event_id}] Extracted books_data from actual_api_response_dict.results.books. Found {len(books_data) if isinstance(books_data, list) else 'N/A'} items.")
    else:
        logger.error(f"[Event ID: {event_id}] Expected actual_api_response_dict to have a 'results' dict containing 'books'. Got: {str(actual_api_response_dict)[:200]}")
        return

    if not isinstance(books_data, list):
        logger.error(f"[Event ID: {event_id}] Expected 'books_data' (from results.books) to be a list, but got {type(books_data)}. Data: {str(books_data)[:200]}")
        return
    
    if not books_data:
        logger.info(f"[Event ID: {event_id}] Received an empty list of books. Nothing to process.")
        return

    book_transformer = NytBookTransformer(event_id)
    transformed_books, skipped_count = book_transformer.transform_data(books_data)

    if not transformed_books:
        logger.info(f"[Event ID: {event_id}] No books to load into BigQuery after transformation (skipped {skipped_count} books).")
        return 

    bigquery_loader = BigQueryLoader(event_id, TABLE_ID)

    merge_keys = ['isbn13', 'bestseller_date', 'list_name','rank','ingest_date']
    
    update_columns = None
    if transformed_books: 
        all_columns = list(transformed_books[0].keys())
        update_columns = [col for col in all_columns if col not in merge_keys]

    success = bigquery_loader.merge_data(
        rows=transformed_books,
        merge_keys=merge_keys,
        update_columns=update_columns
    )

    if success:
        logger.info(f"[Event ID: {event_id}] NYT data processing and loading completed successfully. Loaded {len(transformed_books)} books.")
    else:
        logger.error(f"[Event ID: {event_id}] NYT data processing and loading failed. Check previous logs for details.")