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
# GOOGLE_BOOKS_PUB_SUB_TOPIC is used by the ingest, this subscriber listens to it.
GOOGLE_BOOKS_TABLE_NAME = "google_books_details" # As per google_books_schema.sql
TABLE_ID = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{GOOGLE_BOOKS_TABLE_NAME}"

class GoogleBooksTransformer(BaseTransformer):
    """Transforms Google Books data."""

    @staticmethod
    def _get_isbn_from_identifiers(industry_identifiers, type_name):
        """Helper to extract ISBN from industry identifiers."""
        if not industry_identifiers or not isinstance(industry_identifiers, list):
            return None
        for identifier in industry_identifiers:
            if isinstance(identifier, dict) and identifier.get("type") == type_name:
                return str(identifier.get("identifier", "")).replace("-", "").strip()
        return None

    def _transform_item(self, item, index):
        """Transforms a single Google Book item."""
        volume_info = item.get("volumeInfo", {})
        
        queried_isbn = item.get("_queried_isbn")

        # Determine the best ISBN13 to use
        isbn13_from_identifiers = self._get_isbn_from_identifiers(volume_info.get("industryIdentifiers"), "ISBN_13")
        
        final_isbn13 = None
        if queried_isbn and len(str(queried_isbn).strip()) == 13:
            final_isbn13 = str(queried_isbn).strip()
        elif isbn13_from_identifiers:
            final_isbn13 = isbn13_from_identifiers
        elif queried_isbn: 
             pass

        if not final_isbn13:
            logger.warning(f"[Event ID: {self.event_id}] Missing valid ISBN13 for Google Book at index {index}. Google ID: {item.get('id', 'N/A')}, Queried ISBN: {queried_isbn}. Skipping.")
            return None

        # Prepare additional_metadata: store fields not explicitly mapped to columns
        # These are keys from the root 'item' or 'volumeInfo' whose data is primarily captured in dedicated BQ columns
        # or are internal fields.
        keys_for_main_columns_or_internal_root = {
            "id", "volumeInfo", "_queried_isbn",
            "saleInfo", # Excludes the original saleInfo object from being broken down into additional_metadata
            "accessInfo"
        }
        keys_for_main_columns_volume_info = {
            "title", "subtitle", "authors", "publisher", "publishedDate",
            "description", "pageCount", "categories", "averageRating",
            "ratingsCount", "maturityRating", "language", "previewLink",
            "infoLink", "imageLinks", "industryIdentifiers", # Processed for ISBNs
            "printType", # Mapped to print_type
            "canonicalVolumeLink" # Mapped to canonical_volume_link
        }

        additional_metadata_dict = {}
        for key, value in item.items():
            if key not in keys_for_main_columns_or_internal_root:
                additional_metadata_dict[key] = value
        
        if isinstance(volume_info, dict):
            for vi_key, vi_value in volume_info.items():
                if vi_key not in keys_for_main_columns_volume_info:
                    additional_metadata_dict[f"volumeInfo_{vi_key}"] = vi_value
                   

        additional_metadata_json = json.dumps(additional_metadata_dict) if additional_metadata_dict else None

        original_sale_info_object = item.get("saleInfo")
        sale_info_json_for_bq = json.dumps(original_sale_info_object) if original_sale_info_object else None

        access_info_raw = item.get("accessInfo")
        access_info_json = json.dumps(access_info_raw) if access_info_raw else None

        sale_info_details = item.get("saleInfo", {})
        list_price = sale_info_details.get("listPrice", {})
        list_price_amount = float(list_price.get("amount")) if list_price.get("amount") is not None else None
        list_price_currency_code = list_price.get("currencyCode")
        buy_link = sale_info_details.get("buyLink")

        transformed_book = {
            "ingest_date": self.current_ingest_date,
            "isbn13": final_isbn13,
            "google_id": item.get("id"),
            "title": volume_info.get("title"),
            "subtitle": volume_info.get("subtitle"),
            "authors": [author for author in volume_info.get("authors", []) if isinstance(author, str)] or None, # Changed to None if empty
            "publisher": volume_info.get("publisher"),
            "published_date": volume_info.get("publishedDate"),
            "description": volume_info.get("description"),
            "page_count": int(volume_info.get("pageCount")) if volume_info.get("pageCount") is not None else None, # Changed default to None
            "categories": [cat for cat in volume_info.get("categories", []) if isinstance(cat, str)] or None, # Changed to None if empty
            "average_rating": float(volume_info.get("averageRating")) if volume_info.get("averageRating") is not None else None,
            "ratings_count": int(volume_info.get("ratingsCount")) if volume_info.get("ratingsCount") is not None else None,
            "maturity_rating": volume_info.get("maturityRating"),
            "language": volume_info.get("language"),
            "preview_link": volume_info.get("previewLink"),
            "info_link": volume_info.get("infoLink"),
            "thumbnail_link": volume_info.get("imageLinks", {}).get("thumbnail"),
            "small_thumbnail_link": volume_info.get("imageLinks", {}).get("smallThumbnail"),
            "list_price_amount": list_price_amount,
            "list_price_currency_code": list_price_currency_code,
            "buy_link": buy_link,
            "sale_info_json": sale_info_json_for_bq,
            "access_info": access_info_json,
            "additional_metadata": additional_metadata_json, 
        }
        return transformed_book

@functions_framework.cloud_event
def process_google_books_data_subscriber(cloud_event):
    """Triggered by a Pub/Sub message to process Google Books data and load to BigQuery."""
    event_id = cloud_event.id if hasattr(cloud_event, 'id') else 'N/A'
    event_source = cloud_event.source if hasattr(cloud_event, 'source') else 'N/A'
    logger.info(f"[Event ID: {event_id}] Received Google Books data event from {event_source}")

    message_handler = MessageHandler(event_id)
    books_data_raw = message_handler.decode_and_parse(cloud_event.data)

    if books_data_raw is None:
        return

    if not isinstance(books_data_raw, list):
        logger.error(f"[Event ID: {event_id}] Expected a list of Google Books items, but got {type(books_data_raw)}. Data (first 200 chars): {str(books_data_raw)[:200]}")
        return

    if not books_data_raw:
        logger.info(f"[Event ID: {event_id}] Received an empty list of Google Books items. Nothing to process.")
        return

    book_transformer = GoogleBooksTransformer(event_id)
    transformed_books, skipped_count = book_transformer.transform_data(books_data_raw)

    if not transformed_books:
        logger.info(f"[Event ID: {event_id}] No Google Books items to load into BigQuery after transformation (skipped {skipped_count} items).")
        return

    bigquery_loader = BigQueryLoader(event_id, TABLE_ID)
    
    merge_keys = ['isbn13', 'average_rating', 'ratings_count', 'ingest_date']  # Include ingest_date for partitioning
    update_columns = None # Default to all non-key columns
    if transformed_books:
        all_columns = list(transformed_books[0].keys())
        update_columns = [col for col in all_columns if col not in merge_keys]

    success = bigquery_loader.merge_data(
        rows=transformed_books,
        merge_keys=merge_keys,
        update_columns=update_columns
    )

    if success:
        logger.info(f"[Event ID: {event_id}] Google Books data processing and loading completed successfully. Loaded {len(transformed_books)} items.")
    else:
        logger.error(f"[Event ID: {event_id}] Google Books data processing and loading failed. Check previous logs for details.")
