import os
import json
import requests
import functions_framework
import logging
from typing import List, Dict, Any, Tuple, Optional

from .base_ingest_handlers import PubSubPublisherClient, publish_to_pubsub
from .base_api_client import BaseAPIClient

logger = logging.getLogger(__name__)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
raw_log_level = os.environ.get("LOG_LEVEL", "INFO")
# Strip comments and whitespace from the log level string
clean_log_level = raw_log_level.split('#')[0].strip().strip('"').strip("'")
logger.setLevel(clean_log_level.upper())

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "realtime-book-data-gcp")
GB_PUB_SUB_TOPIC_NAME = os.environ.get("GOOGLE_BOOKS_PUBSUB_TOPIC_NAME", "gcpbooks-raw-data")
GOOGLE_BOOKS_API_URL_CONST = "https://www.googleapis.com/books/v1/volumes"

if not GB_PUB_SUB_TOPIC_NAME.startswith("projects/"):
    GB_TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{GB_PUB_SUB_TOPIC_NAME}"
else:
    GB_TOPIC_PATH = GB_PUB_SUB_TOPIC_NAME

class GoogleBooksAPIClient(BaseAPIClient):
    def __init__(self, api_key: str, base_url: str = GOOGLE_BOOKS_API_URL_CONST):
        super().__init__(base_url=base_url, api_key=api_key)

    def fetch_books_by_isbns(self, isbn_list: List[str], sub_batch_size: int = 5) -> List[Dict[str, Any]]:
        if not isbn_list:
            logger.info("API Client: ISBN list is empty for Google Books. Returning empty list.")
            return []
        
        logger.info(f"API Client: Processing {len(isbn_list)} ISBNs for Google Books in sub-batches of {sub_batch_size}.")
        all_book_items_aggregated: List[Dict[str, Any]] = []

        for i in range(0, len(isbn_list), sub_batch_size):
            sub_batch_isbns = isbn_list[i:i + sub_batch_size]
            logger.info(f"API Client: Processing sub-batch of ISBNs: {sub_batch_isbns}")
            
            current_sub_batch_fetched_items: List[Dict[str, Any]] = []
            for isbn_query in sub_batch_isbns:
                params = {
                    "q": f"isbn:{isbn_query}",
                    "key": self.api_key
                }
                logger.info(f"API Client: Fetching data from Google Books API for ISBN: {isbn_query} with URL: {self.base_url}")
                
                data, error = self._make_request(self.base_url, params=params, timeout=15)
                if error:
                    logger.error(f"API Client: Failed to fetch data for ISBN {isbn_query}: {error}")
                    continue
                
                if data and data.get("totalItems", 0) > 0 and "items" in data:
                    items_for_current_isbn = data.get("items", [])
                    logger.info(f"API Client: Successfully fetched {len(items_for_current_isbn)} item(s) for ISBN {isbn_query}.")
                    for item in items_for_current_isbn:
                        item["_queried_isbn"] = isbn_query
                    current_sub_batch_fetched_items.extend(items_for_current_isbn)
                elif data:
                    logger.info(f"API Client: No items found for ISBN {isbn_query}. API Response snippet: {str(data)[:200]}")
            
            if current_sub_batch_fetched_items:
                logger.info(f"API Client: Fetched {len(current_sub_batch_fetched_items)} items for sub-batch {sub_batch_isbns}.")
                all_book_items_aggregated.extend(current_sub_batch_fetched_items)
            else:
                logger.info(f"API Client: No items fetched for sub-batch {sub_batch_isbns}.")

        logger.info(f"API Client: Completed fetching for all ISBNs. Total items aggregated: {len(all_book_items_aggregated)}.")
        return all_book_items_aggregated

@functions_framework.http
def google_books_http_and_pubsub_trigger(request) -> Any:
    logger.info(f"HTTP POST Trigger: Received request for Google Books. Path={request.path}, IP={request.remote_addr}")

    if request.method != "POST":
        logger.error(f"HTTP Trigger: Invalid request method: {request.method}. Only POST is supported.")
        return {"error": "Invalid request method. Only POST is supported.", "status": "METHOD_NOT_ALLOWED"}, 405

    payload = None
    isbns_str = None

    try:
        content_type = request.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            logger.error(f"HTTP POST Trigger: Invalid Content-Type: {content_type}. Expected 'application/json'.")
            return {"error": "Invalid Content-Type. Expected 'application/json'.", "status": "BAD_REQUEST"}, 400

        payload_str = request.data.decode('utf-8')
        if not payload_str:
            logger.error("HTTP POST Trigger: Empty request body.")
            return {"error": "Empty request body.", "status": "BAD_REQUEST"}, 400
        
        payload = json.loads(payload_str)
        if isinstance(payload, dict):
            isbns_str = payload.get("isbns")
        else:
            logger.error(f"HTTP POST Trigger: Invalid JSON payload type. Expected a JSON object. Got: {type(payload)}")
            return {"error": "Invalid JSON payload type. Expected a JSON object.", "status": "BAD_REQUEST"}, 400

    except json.JSONDecodeError as e:
        logger.error(f"HTTP POST Trigger: Error decoding JSON request body: {e}. Data (first 200 chars): {request.data[:200]}")
        return {"error": "Invalid JSON input_data. Failed to decode.", "status": "PARSE_ERROR"}, 400
    except Exception as e:
        logger.error(f"HTTP POST Trigger: Error processing request data: {e}. Data (first 200 chars): {request.data[:200]}")
        return {"error": f"Error processing request data: {e}", "status": "INTERNAL_SERVER_ERROR"}, 500

    if not isbns_str:
        error_msg = "Missing or empty 'isbns' key in JSON payload."
        logger.error(f"HTTP POST Trigger: {error_msg}")
        return {"error": error_msg, "status": "BAD_REQUEST"}, 400

    if not isinstance(isbns_str, str):
        error_msg = f"'isbns' key must contain a comma-separated string of ISBNs. Got: {type(isbns_str)}"
        logger.error(f"HTTP POST Trigger: {error_msg}")
        return {"error": error_msg, "status": "BAD_REQUEST"}, 400

    isbns_param = [isbn.strip() for isbn in isbns_str.split(",") if isbn.strip()]
    if not isbns_param:
        error_msg = "No valid ISBNs provided in 'isbns' parameter after parsing and stripping."
        logger.error(f"HTTP POST Trigger: {error_msg}")
        return {"error": error_msg, "status": "BAD_REQUEST"}, 400

    logger.info(f"HTTP POST Trigger: Processing for {len(isbns_param)} ISBNs: {isbns_param[:5]}...")

    cf_api_key = os.environ.get("GOOGLE_BOOKS_API_KEY")
    if not cf_api_key:
        error_msg = "GOOGLE_BOOKS_API_KEY environment variable not set."
        logger.error(f"HTTP POST Trigger: Critical config error - {error_msg}")
        return {"error": error_msg, "status": "CONFIG_ERROR"}, 500

    api_client = GoogleBooksAPIClient(api_key=cf_api_key)
    all_fetched_book_items = [] # Initialize to empty list
    try:
        all_fetched_book_items = api_client.fetch_books_by_isbns(
            isbn_list=isbns_param,
            sub_batch_size=5 
        )
        logger.info(f"HTTP POST Trigger: API client finished fetching. Total items fetched: {len(all_fetched_book_items)}.")
    except Exception as e: # Catch potential errors from fetch_books_by_isbns itself, though most are handled by _make_request
        logger.error(f"HTTP POST Trigger: Error during Google Books data fetch for ISBNs: {isbns_param[:5]}...: {e}", exc_info=True)
        return json.dumps({"error": f"Failed to fetch data from Google Books API: {str(e)}", "status": "API_FETCH_ERROR"}), 500, {"Content-Type": "application/json"}

    if not all_fetched_book_items:
        logger.info(f"HTTP POST Trigger: No book items found for the provided ISBNs: {isbns_param[:5]}... Returning empty list.")
        # Still publish an empty list to Pub/Sub if needed, or handle as per requirements.
        # For now, will proceed to publish, and publisher can handle empty list.

    logger.info(f"HTTP POST Trigger: Fetched {len(all_fetched_book_items)} book items. Attempting to publish to {GB_TOPIC_PATH}.")
    
    success, pubsub_error = publish_to_pubsub(
        data=all_fetched_book_items,
        topic_path=GB_TOPIC_PATH,
        logger=logger
    )

    if success:
        logger.info(f"HTTP POST Trigger: Successfully published {len(all_fetched_book_items)} items to Pub/Sub topic {GB_TOPIC_PATH}.")
        return json.dumps(all_fetched_book_items), 200, {"Content-Type": "application/json"}
    else:
        logger.error(f"HTTP POST Trigger: Failed to publish to Pub/Sub. Error: {pubsub_error}. Data count: {len(all_fetched_book_items)}")
        return json.dumps({
            "error": pubsub_error,
            "status": "PUBSUB_PUBLISH_FAILED",
            "data_fetched_count": len(all_fetched_book_items)
        }), 500, {"Content-Type": "application/json"}

def fetch_google_books_data(api_key: str, isbn_list: List[str]) -> List[Dict[str, Any]]:
    logger.info(f"Direct Call: fetch_google_books_data for {len(isbn_list)} ISBNs")
    if not api_key:
        logger.error("Direct Call: API key must be provided for fetch_google_books_data.")
        return []
        
    api_client = GoogleBooksAPIClient(api_key=api_key)
    try:
        return api_client.fetch_books_by_isbns(isbn_list=isbn_list)
    except Exception as e:
        logger.error(f"Direct Call: Error fetching Google Books data: {e}", exc_info=True)
        return []