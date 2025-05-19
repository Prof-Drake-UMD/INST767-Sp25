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
OL_PUB_SUB_TOPIC_NAME = os.environ.get("OPEN_LIBRARY_PUBSUB_TOPIC_NAME", "open-library-raw-data")

if not OL_PUB_SUB_TOPIC_NAME.startswith("projects/"):
    OL_TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{OL_PUB_SUB_TOPIC_NAME}"
else:
    OL_TOPIC_PATH = OL_PUB_SUB_TOPIC_NAME

OPEN_LIBRARY_API_URL_CONST = "https://openlibrary.org/api/books"

class OpenLibraryAPIClient(BaseAPIClient):
    def __init__(self, api_url: str = OPEN_LIBRARY_API_URL_CONST):
        super().__init__(base_url=api_url)

    def fetch_books_by_isbns(self, isbn_list: List[str]) -> List[Dict[str, Any]]:
        if not isbn_list:
            logger.info("API Client: ISBN list is empty. Returning empty list.")
            return []

        logger.info(f"API Client: Processing {len(isbn_list)} ISBNs: {isbn_list}")
        params = {
            "bibkeys": ",".join([f"ISBN:{isbn}" for isbn in isbn_list]),
            "format": "json",
            "jscmd": "data"
        }
        all_books_details: List[Dict[str, Any]] = []

        data, error = self._make_request(self.base_url, params=params, timeout=20)
        if error:
            logger.error(f"API Client: Failed to fetch data: {error}")
            return []
            
        for bibkey, book_detail_raw in data.items():
            if isinstance(book_detail_raw, dict):
                actual_isbn = bibkey.replace("ISBN:", "") if bibkey.startswith("ISBN:") else bibkey
                book_detail_processed = book_detail_raw.copy()
                book_detail_processed["_queried_isbn"] = actual_isbn
                all_books_details.append(book_detail_processed)
            else:
                logger.warning(f"API Client: Item for bibkey {bibkey} in response is not a dictionary, skipping. Value: {str(book_detail_raw)[:100]}")
        
        logger.info(f"API Client: Successfully processed data for {len(all_books_details)} items. Original API response had {len(data)} keys.")
        if len(all_books_details) != len(data) or (isbn_list and len(data) != len(isbn_list)):
             logger.info(f"API Client: Requested {len(isbn_list)} ISBNs, API returned data for {len(data)} bibkeys, processed {len(all_books_details)} book entries.")
        return all_books_details

@functions_framework.http
def open_library_http_and_pubsub_trigger(request) -> Any:
    """HTTP-triggered function that fetches Open Library data for given ISBNs and publishes to Pub/Sub.
       Expects an HTTP POST request with a JSON payload like: {"isbns": "isbn1,isbn2,..."}.
    """
    logger.info("Open Library Ingest Function: Triggered by HTTP request.")
    
    payload = None
    isbns_str = None

    if request.method == "POST":
        logger.info(f"HTTP Trigger Details: Method={request.method}, Path={request.path}, IP={request.remote_addr}")
        try:
            payload_str = request.data.decode('utf-8')
            if payload_str:
                payload = json.loads(payload_str)
                logger.info(f"Received payload: {payload}")
                if isinstance(payload, dict):
                    isbns_str = payload.get("isbns")
            else:
                logger.warning("HTTP POST request received with empty body.")
                return json.dumps({"error": "Request body is empty", "status": "BAD_REQUEST"}), 400, {"Content-Type": "application/json"}
        except Exception as e:
            logger.error(f"HTTP Trigger: Could not parse POST data as JSON: {e}", exc_info=True)
            return json.dumps({"error": "Invalid JSON in request body", "status": "BAD_REQUEST"}), 400, {"Content-Type": "application/json"}
    else:
        logger.warning(f"HTTP Trigger: Received non-POST request method: {request.method}")
        return json.dumps({"error": "Only POST requests are accepted", "status": "METHOD_NOT_ALLOWED"}), 405, {"Content-Type": "application/json"}

    if not isbns_str:
        error_msg = "Missing or empty 'isbns' parameter in JSON payload."
        logger.error(f"Trigger: {error_msg}")
        return json.dumps({"error": error_msg, "status": "BAD_REQUEST"}), 400, {"Content-Type": "application/json"}

    isbns_list = [isbn.strip() for isbn in isbns_str.split(",") if isbn.strip()]
    if not isbns_list:
        error_msg = "No valid ISBNs provided in 'isbns' parameter after stripping/splitting."
        logger.error(f"Trigger: {error_msg}")
        return json.dumps({"error": error_msg, "status": "BAD_REQUEST"}), 400, {"Content-Type": "application/json"}
    
    logger.info(f"Trigger: Processing for {len(isbns_list)} ISBNs: {isbns_list[:5]}...")

    api_client = OpenLibraryAPIClient()
    books_details: List[Dict[str, Any]] = []
    try:
        books_details = api_client.fetch_books_by_isbns(isbn_list=isbns_list)
    except Exception as e:
        logger.error(f"Trigger: Error during Open Library data fetch for ISBNs: {isbns_list[:5]}...: {e}", exc_info=True)
        return json.dumps({"error": f"Failed to fetch data from Open Library API: {str(e)}", "status": "API_FETCH_ERROR"}), 500, {"Content-Type": "application/json"}

    if not books_details:
        # This isn't necessarily an error; the ISBNs might not exist in Open Library.
        logger.info(f"Trigger: No book details found for the provided ISBNs: {isbns_list[:5]}... Returning empty list.")
        # Continue to publish an empty list, subscriber can handle it or it signals no data found.
    else:
        logger.info(f"Trigger: Fetched {len(books_details)} book details. Attempting to publish.")

    success, error = publish_to_pubsub(
        data=books_details,
        topic_path=OL_TOPIC_PATH,
        logger=logger
    )

    if success:
        return json.dumps(books_details), 200, {"Content-Type": "application/json"}
    else:
        return json.dumps({
            "error": error,
            "status": "PUBSUB_PUBLISH_FAILED",
            "data_fetched_count": len(books_details)
        }), 500, {"Content-Type": "application/json"}

def fetch_open_library_books(isbn_list: List[str]) -> List[Dict[str, Any]]:
    logger.info(f"Direct Call: fetch_open_library_books for {len(isbn_list)} ISBNs")
    api_client = OpenLibraryAPIClient()
    try:
        return api_client.fetch_books_by_isbns(isbn_list=isbn_list)
    except Exception as e:
        logger.error(f"Direct Call: Error fetching Open Library books: {e}", exc_info=True)
        raise