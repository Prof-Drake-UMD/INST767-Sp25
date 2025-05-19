import os
import logging
import json

import functions_framework

from .base_handler import MessageHandler, BaseTransformer, BigQueryLoader

# Configure logger for this specific subscriber module
logger = logging.getLogger(__name__)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Environment variables specific to this subscriber
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gcp-project-id")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET_NAME", "dataset")
OPEN_LIBRARY_TABLE_NAME = "open_library_books" 
TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{OPEN_LIBRARY_TABLE_NAME}"

class OpenLibraryBookTransformer(BaseTransformer):
    """Transforms Open Library book data."""

    def _transform_item(self, book_detail, index):
        """Transforms a single Open Library book item."""
        # book_detail is an item from the list provided by the OpenLibrary API client.
        # It includes both the OpenLibrary data and the '_queried_isbn' added by the fetcher.
        
        # 'details' will point to the dictionary containing the actual book metadata.
        # Based on the sample, OpenLibrary data is at the top level of book_detail, not nested under a "details" key.
        # If there was a chance it was nested, book_detail.get("details", book_detail) would be safer.
        # For simplicity with the provided sample, we'll assume details = book_detail.
        details = book_detail 

        queried_isbn_from_input = details.get("_queried_isbn")

        # Extract ISBN-13, prioritizing it from the 'identifiers' field
        isbn_13_list = [str(isbn).replace("-", "").strip() for isbn in details.get("identifiers", {}).get("isbn_13", []) if isbn]
        final_isbn13 = isbn_13_list[0] if isbn_13_list else None

        # If ISBN-13 is not found in 'identifiers', try to use '_queried_isbn' if it's a 13-digit number
        if not final_isbn13:
            if queried_isbn_from_input and len(str(queried_isbn_from_input).strip()) == 13:
                final_isbn13 = str(queried_isbn_from_input).strip()

        # If a valid ISBN-13 cannot be determined, skip the item
        if not final_isbn13:
            logger.warning(f"[Event ID: {self.event_id}] Missing valid ISBN13 for Open Library item at index {index} (Queried ISBN: {queried_isbn_from_input}). Title: '{details.get('title')}'. Skipping.")
            return None

        # Authors: array of names
        authors_data = details.get("authors", [])
        authors_names = [str(author.get("name")) for author in authors_data if isinstance(author, dict) and author.get("name")] or None

        # Publishers: array of names
        publishers_data = details.get("publishers", [])
        publishers_names = [str(publisher.get("name")) for publisher in publishers_data if isinstance(publisher, dict) and publisher.get("name")] or None
        
        # Publish Places: array of names
        publish_places_data = details.get("publish_places", [])
        publish_places_names = [str(place.get("name")) for place in publish_places_data if isinstance(place, dict) and place.get("name")] or None

        # Subjects: array of names
        subjects_data = details.get("subjects", [])
        subjects_names = [str(subject.get("name")) for subject in subjects_data if isinstance(subject, dict) and subject.get("name")] or None

        # Cover URL: prefer large, then medium, then small
        cover_data = details.get("cover")
        cover_url = None
        if isinstance(cover_data, dict):
            cover_url = cover_data.get("large") or cover_data.get("medium") or cover_data.get("small")

        # Number of pages: ensure it's an integer
        num_pages_raw = details.get("number_of_pages")
        number_of_pages_int = None
        if num_pages_raw is not None:
            try:
                number_of_pages_int = int(num_pages_raw)
            except (ValueError, TypeError):
                logger.warning(f"[Event ID: {self.event_id}] Could not convert number_of_pages '{num_pages_raw}' to int for ISBN {final_isbn13}. Setting to None.")

        # Prepare additional_metadata: store fields not explicitly mapped to columns
        additional_metadata_dict = {}
        # These are keys from 'details' whose data is primarily captured in dedicated BQ columns
        # or are internal fields.
        keys_for_main_columns_or_internal = {
            "title", "subtitle", "authors", "publish_date", "publishers", 
            "publish_places", # Added to exclude from additional_metadata
            "number_of_pages", "subjects", "cover", 
            "_queried_isbn", # Internal field added by fetcher
            # 'identifiers' is used for isbn13, but the full dict is good for additional_metadata
        }

        for key, value in details.items():
            if key not in keys_for_main_columns_or_internal:
                additional_metadata_dict[key] = value
        
        # Ensure 'identifiers' (which contains various ISBNs, LCCN, etc.) is in additional_metadata
        # if it wasn't excluded and it exists in details.
        if "identifiers" in details and "identifiers" not in additional_metadata_dict:
             additional_metadata_dict["identifiers"] = details.get("identifiers")


        additional_metadata_json = json.dumps(additional_metadata_dict) if additional_metadata_dict else None
        
        transformed_book = {
            "ingest_date": self.current_ingest_date,
            "isbn13": final_isbn13,
            "title": details.get("title"),
            "subtitle": details.get("subtitle"), # Will be None if not present
            "authors": authors_names,
            "publish_date": details.get("publish_date"), # Assumed to be a string as per schema
            "publishers": publishers_names,
            "publish_places": publish_places_names, # Added new field
            "number_of_pages": number_of_pages_int,
            "subjects": subjects_names,
            "cover_url": cover_url,
            "additional_metadata": additional_metadata_json,
        }
        return transformed_book

@functions_framework.cloud_event
def process_open_library_data_subscriber(cloud_event):
    """Triggered by a Pub/Sub message to process Open Library data and load to BigQuery."""
    event_id = cloud_event.id if hasattr(cloud_event, 'id') else 'N/A'
    event_source = cloud_event.source if hasattr(cloud_event, 'source') else 'N/A'
    logger.info(f"[Event ID: {event_id}] Received Open Library data event from {event_source}")

    message_handler = MessageHandler(event_id)
    books_data_raw = message_handler.decode_and_parse(cloud_event.data)

    if books_data_raw is None: 
        return
    
    if not isinstance(books_data_raw, list):
        logger.error(f"[Event ID: {event_id}] Expected a list of Open Library items, but got {type(books_data_raw)}. Data (first 200 chars): {str(books_data_raw)[:200]}")
        return
    
    if not books_data_raw:
        logger.info(f"[Event ID: {event_id}] Received an empty list of Open Library items. Nothing to process.")
        return

    book_transformer = OpenLibraryBookTransformer(event_id)
    transformed_books, skipped_count = book_transformer.transform_data(books_data_raw)

    if not transformed_books:
        logger.info(f"[Event ID: {event_id}] No Open Library books to load into BigQuery after transformation (skipped {skipped_count} items).")
        return

    bigquery_loader = BigQueryLoader(event_id, TABLE_ID)

    merge_keys = ['isbn13', 'ingest_date']
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
        logger.info(f"[Event ID: {event_id}] Open Library data processing and loading completed successfully. Loaded {len(transformed_books)} items.")
    else:
        logger.error(f"[Event ID: {event_id}] Open Library data processing and loading failed. Check previous logs for details.")
