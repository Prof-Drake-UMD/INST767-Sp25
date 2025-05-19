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

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "realtime-book-data-gcp")
NYT_PUB_SUB_TOPIC_NAME = os.environ.get("NYT_PUBSUB_TOPIC_NAME", "nyt-books-raw-data")
NYT_API_URL_CONST = "https://api.nytimes.com/svc/books/v3"

if not NYT_PUB_SUB_TOPIC_NAME.startswith("projects/"):
    NYT_TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{NYT_PUB_SUB_TOPIC_NAME}"
else:
    NYT_TOPIC_PATH = NYT_PUB_SUB_TOPIC_NAME

class NYTAPIClient(BaseAPIClient):
    def __init__(self, api_key: str, base_url: str = NYT_API_URL_CONST):
        super().__init__(base_url=base_url, api_key=api_key)

    def fetch_bestsellers_for_list(self, list_name_encoded: str) -> Optional[Dict[str, Any]]:
        endpoint = f"{self.base_url}/lists/current/{list_name_encoded}.json"
        params = {"api-key": self.api_key}
        logger.info(f"API Client: Fetching bestsellers for list: {list_name_encoded} from {endpoint}")
        
        data, error = self._make_request(endpoint, params=params, timeout=20)
        if error:
            logger.error(f"API Client: Failed to fetch data for list {list_name_encoded}: {error}")
            return None
            
        logger.info(f"API Client: Successfully fetched data for list {list_name_encoded}. Num_results: {data.get('num_results', 0)}")
        
        # Add list_name_encoded and published_date to each book for context
        if data and "results" in data and "books" in data["results"]:
            published_date = data["results"].get("published_date")
            list_name_from_api = data["results"].get("list_name") # Keep original list name if available
            for book in data["results"]["books"]:
                book["_bestseller_date"] = published_date
                book["_list_name_encoded"] = list_name_encoded 
                book["_list_name_from_api"] = list_name_from_api
        return data

    def fetch_bestsellers_for_multiple_lists(self, list_names_encoded: List[str]) -> List[Dict[str, Any]]:
        all_results = []
        for list_name_encoded in list_names_encoded:
            result = self.fetch_bestsellers_for_list(list_name_encoded)
            if result:
                all_results.append(result)
        return all_results

@functions_framework.http
def nyt_http_and_pubsub_trigger(request) -> Any:
    """HTTP-triggered function that fetches NYT data and publishes to Pub/Sub.
       It expects an HTTP POST request with a JSON payload like: {"list": "list-name-encoded"}.
    """
    logger.info("NYT Ingest Function: Triggered by HTTP request.")
    
    payload = None
    list_name_param = None

    if request.method == "POST":
        logger.info(f"HTTP Trigger Details: Method={request.method}, Path={request.path}, IP={request.remote_addr}")
        try:
            payload_str = request.data.decode('utf-8')
            if payload_str:
                payload = json.loads(payload_str)
                logger.info(f"Received payload: {payload}")
            else:
                logger.warning("HTTP POST request received with empty body.")
                return json.dumps({"error": "Request body is empty", "status": "BAD_REQUEST"}), 400, {"Content-Type": "application/json"}
        except Exception as e:
            logger.error(f"HTTP Trigger: Could not parse POST data as JSON: {e}", exc_info=True)
            return json.dumps({"error": "Invalid JSON in request body", "status": "BAD_REQUEST"}), 400, {"Content-Type": "application/json"}
        
        if payload:
            list_name_param = payload.get("list")
    else:
        logger.warning(f"HTTP Trigger: Received non-POST request method: {request.method}")
        return json.dumps({"error": "Only POST requests are accepted", "status": "METHOD_NOT_ALLOWED"}), 405, {"Content-Type": "application/json"}

    if not list_name_param:
        logger.error("HTTP Trigger: 'list' parameter not found in JSON payload.")
        return json.dumps({"error": "'list' parameter missing in JSON payload", "status": "BAD_REQUEST"}), 400, {"Content-Type": "application/json"}

    list_name_to_fetch = list_name_param
    logger.info(f"Processing for list: {list_name_to_fetch}")

    cf_api_key = os.environ.get("NYT_API_KEY")
    if not cf_api_key:
        error_msg = "NYT_API_KEY environment variable not set."
        logger.error(f"Trigger: {error_msg}")
        return json.dumps({"error": error_msg, "status": "CONFIG_ERROR"}), 500, {"Content-Type": "application/json"}

    api_client = NYTAPIClient(api_key=cf_api_key)
    
    fetched_list_data: Optional[Dict[str, Any]] = None
    logger.info(f"Trigger: Processing single list: {list_name_to_fetch}")
    try:
        fetched_list_data = api_client.fetch_bestsellers_for_list(list_name_encoded=list_name_to_fetch)
        logger.info(f"Trigger: Fetched data {fetched_list_data}")
    except Exception as e: 
        logger.error(f"Error fetching data for list {list_name_to_fetch} from NYTAPIClient: {e}", exc_info=True)
        return json.dumps({"error": f"Failed to fetch data from NYT API for list {list_name_to_fetch}", "details": str(e), "status": "API_FETCH_ERROR"}), 500, {"Content-Type": "application/json"}

    if not fetched_list_data:
        warning_msg = f"No data fetched from NYT API for the specified list: {list_name_to_fetch}. This might be expected if the list is empty or invalid."
        logger.warning(f"Trigger: {warning_msg}")
        return json.dumps({"status": "NO_DATA", "message": warning_msg, "results": {"books": []}}), 200, {"Content-Type": "application/json"}

    logger.info(f"Trigger: Fetched data for list {list_name_to_fetch}. Attempting to publish.")
    
    actual_list_name_in_msg = fetched_list_data.get("results", {}).get("list_name", list_name_to_fetch)
    logger.info(f"Trigger: Actual list name in message: {actual_list_name_in_msg}")
    if isinstance(fetched_list_data.get("results"), dict) and "books" in fetched_list_data["results"]:
        num_books_in_msg = len(fetched_list_data["results"]["books"])
        logger.info(f"Trigger: Data for list '{actual_list_name_in_msg}' contains {num_books_in_msg} books in 'results.books'. Publishing this full structure.")
        if num_books_in_msg > 0 and isinstance(fetched_list_data["results"]["books"][0], dict):
            first_book_title = fetched_list_data["results"]["books"][0].get('title', 'N/A')
            logger.info(f"Trigger: First book title in data: {first_book_title}")
    else:
        logger.warning(f"Trigger: Fetched data for list '{actual_list_name_in_msg}' does not have the expected 'results.books' structure. Data (first 300 chars): {str(fetched_list_data)[:300]}")

    message_to_publish_for_pubsub = [fetched_list_data]

    success, error = publish_to_pubsub(
        data=message_to_publish_for_pubsub,
        topic_path=NYT_TOPIC_PATH,
        logger=logger
    )

    if success:
        return json.dumps(fetched_list_data), 200, {"Content-Type": "application/json"}
    else:
        return json.dumps({
            "error": error,
            "status": "PUBSUB_PUBLISH_FAILED",
            "data_fetched": fetched_list_data is not None
        }), 500, {"Content-Type": "application/json"}

def fetch_nyt_bestsellers(api_key: str, list_name_encoded: str) -> Optional[Dict[str, Any]]:
    """
    Direct callable function to fetch bestsellers for a single list.
    This is intended for direct Python calls if needed, bypassing HTTP/PubSub triggers.
    """
    logger.info(f"Direct Call: fetch_nyt_bestsellers for list: {list_name_encoded}")
    if not api_key:
        logger.error("Direct Call: NYT API key must be provided.")
        return None
        
    client = NYTAPIClient(api_key=api_key)
    try:
        return client.fetch_bestsellers_for_list(list_name_encoded)
    except Exception as e:
        logger.error(f"Direct Call: Error fetching NYT bestsellers for {list_name_encoded}: {e}", exc_info=True)
        return None

def fetch_nyt_bestsellers_for_multiple_lists(api_key: str, list_names_encoded: List[str]) -> List[Dict[str, Any]]:
    """
    Direct callable function to fetch bestsellers for multiple lists.
    """
    logger.info(f"Direct Call: fetch_nyt_bestsellers_for_multiple_lists for {len(list_names_encoded)} lists.")
    if not api_key:
        logger.error("Direct Call: NYT API key must be provided.")
        return []
    
    client = NYTAPIClient(api_key=api_key)
    try:
        return client.fetch_bestsellers_for_multiple_lists(list_names_encoded=list_names_encoded)
    except Exception as e:
        logger.error(f"Direct Call: Error fetching NYT bestsellers for multiple lists: {e}", exc_info=True)
        return []