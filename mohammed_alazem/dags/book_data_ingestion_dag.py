import import_helper
import json
import logging
from datetime import datetime, timedelta
import time
import traceback
from typing import List, Dict, Any

import requests
from googleapiclient.discovery import build

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Configure root logger for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("book_data_ingest")

# --- DAG Configuration ---
DAG_ID = "book_data_ingestion_pipeline_v2"
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=60),
}

# GCP configuration
GCP_PROJECT_ID = "realtime-book-data-gcp"
GCP_LOCATION = "us-central1"
NYT_FUNCTION_NAME = "nyt-ingest-http"
OL_FUNCTION_NAME = "open-library-ingest-http"
GB_FUNCTION_NAME = "google-books-ingest-http"

# API batch configuration
ISBN_BATCH_SIZE = 30  # Number of ISBNs per batch for API calls

# NYT API batches to avoid rate limiting (30 calls per minute)
NYT_BESTSELLER_BATCHES = [
    ["hardcover-fiction", "hardcover-nonfiction", "paperback-nonfiction", "picture-books", "series-books"],
    ["childrens-middle-grade-hardcover", "young-adult-hardcover", "trade-fiction-paperback", "advice-how-to-and-miscellaneous", "combined-print-and-e-book-fiction"],
    ["combined-print-and-e-book-nonfiction", "middle-grade-paperback-monthly", "young-adult-paperback-monthly", "mass-market-monthly"],
    ["audio-fiction", "audio-nonfiction", "business-books", "graphic-books-and-manga"]
]

# Flat list for aggregation
NYT_BESTSELLER_LISTS = [list_name for batch in NYT_BESTSELLER_BATCHES for list_name in batch]

# --- Cloud Function URL Resolution ---
def resolve_cf_url(project_id, location, function_name):
    """Resolve a Cloud Function URL from its name and location."""
    service = build("cloudfunctions", "v2")
    name = f"projects/{project_id}/locations/{location}/functions/{function_name}"
    
    try:
        response = service.projects().locations().functions().get(name=name).execute()
        url = response.get("serviceConfig", {}).get("uri")
        if not url:
            error_msg = f"URI not found for function {name}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        return url
    except Exception as e:
        logger.error(f"Error resolving URL for {function_name}: {e}", exc_info=True)
        raise

def fetch_and_store_url(project_id: str, location: str, function_name: str, xcom_key: str, **kwargs):
    """Fetch a Cloud Function URL and store it in XCom."""
    url = resolve_cf_url(project_id, location, function_name)
    ti = kwargs['ti']
    ti.xcom_push(key=xcom_key, value=url)
    logger.info(f"Stored {function_name} URL in XCom key: {xcom_key}")
    return url

# --- API Invocation Functions ---
def call_api(function_url: str, payload: Dict[str, Any], max_retries: int = 3, initial_backoff: int = 5) -> Any:
    """Generic function to call an API with JSON payload, with retries for 429 errors."""
    retries = 0
    backoff_seconds = initial_backoff
    while retries <= max_retries:
        try:
            response = requests.post(function_url, json=payload, timeout=180)
            logger.info(f"API response status: {response.status_code} for {function_url}")

            if response.status_code == 429:
                logger.warning(f"Received 429 (Too Many Requests) from {function_url}.")
                if retries == max_retries:
                    logger.error(f"Max retries ({max_retries}) reached for {function_url}. Raising error.")
                    response.raise_for_status()
                
                logger.info(f"Retrying in {backoff_seconds} seconds... (Attempt {retries + 1}/{max_retries})")
                time.sleep(backoff_seconds)
                backoff_seconds *= 2
                retries += 1
                continue

            if response.status_code != 200:
                logger.warning(f"API error response from {function_url}: {response.text[:1000]}")
            response.raise_for_status()
            
            try:
                json_response = response.json()
                if isinstance(json_response, list):
                    logger.info(f"API ({function_url}) returned list with {len(json_response)} items")
                return json_response
            except json.JSONDecodeError:
                logger.info(f"API ({function_url}) response was not JSON: {response.text[:500]}")
                return response.text

        except requests.exceptions.RequestException as e:
            logger.error(f"RequestException during API call to {function_url}: {e}", exc_info=True)
            if retries == max_retries:
                logger.error(f"Max retries reached after RequestException for {function_url}. Raising.")
                raise
            logger.info(f"Retrying in {backoff_seconds} seconds due to RequestException... (Attempt {retries + 1}/{max_retries})")
            time.sleep(backoff_seconds)
            backoff_seconds *= 2
            retries += 1
    
    raise Exception(f"Failed to call API {function_url} after {max_retries} retries")

def fetch_nyt_bestseller_list(list_name, batch_index, list_index, **kwargs):
    """Fetch a single NYT bestseller list."""
    ti = kwargs['ti']
    try:
        url = ti.xcom_pull(task_ids="get_nyt_function_url", key="nyt_function_url")
        
        if not url:
            error_msg = "NYT function URL not found in XComs"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        delay_seconds = list_index * 3.0
        logger.info(f"Delaying {delay_seconds}s before calling NYT API for list: {list_name}")
        time.sleep(delay_seconds)
        
        try:
            payload = {"list": list_name}
            response = call_api(url, payload)
            
            xcom_key = f"nyt_response_for_{list_name.replace('-', '_')}"
            ti.xcom_push(key=xcom_key, value=response)
            logger.info(f"Pushed response to XCom with key: {xcom_key}")
            
            return response
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for NYT list {list_name}: {e}")
            if hasattr(e, 'response') and e.response.status_code == 429:
                retry_delay = 10 + (batch_index * 5)
                logger.warning(f"Rate limit hit for {list_name}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
                response = call_api(url, payload)
                ti.xcom_push(key=f"nyt_response_for_{list_name.replace('-', '_')}", value=response)
                logger.info(f"Successfully processed NYT list {list_name} after retry")
                return response
            else:
                raise
    except Exception as e:
        logger.error(f"Error in fetch_nyt_bestseller_list for {list_name}: {e}", exc_info=True)
        raise

def process_nyt_batch(batch_idx, lists, **kwargs):
    """Process a batch of NYT bestseller lists."""
    logger.info(f"Starting process_nyt_batch for batch {batch_idx} with {len(lists)} lists")
    results = []
    
    for j, list_name in enumerate(lists):
        logger.info(f"Processing list {j+1}/{len(lists)}: {list_name}")
        try:
            result = fetch_nyt_bestseller_list(
                list_name=list_name,
                batch_index=batch_idx,
                list_index=j,
                **kwargs
            )
            results.append(result)
            logger.info(f"Successfully processed list: {list_name}")
        except Exception as e:
            logger.error(f"Error processing list {list_name}: {e}", exc_info=True)
            results.append(None)
    
    logger.info(f"Completed processing of batch {batch_idx}, processed {len(results)} lists")
    return results

def wait_between_batches(batch_index, **kwargs):
    """Add delay between NYT API call batches."""
    delay_seconds = 45
    logger.info(f"Waiting {delay_seconds}s between NYT API batches (after batch {batch_index})")
    time.sleep(delay_seconds)
    return f"Completed wait for batch {batch_index}"

# --- ISBN Processing Functions ---
def extract_unique_isbns(**kwargs):
    """Extract unique ISBNs from all NYT bestseller lists."""
    ti = kwargs["ti"]
    all_isbns = set()
    processed_lists = 0
    errors_pulling_xcom = 0
    
    all_batch_results = {}
    for batch_idx, batch_of_list_names in enumerate(NYT_BESTSELLER_BATCHES):
        producer_task_id = f"nyt_api_calls.nyt_batch_{batch_idx}.process_nyt_batch"
        
        for list_name in batch_of_list_names:
            safe_list_name = list_name.replace('-', '_')
            xcom_key = f"nyt_response_for_{safe_list_name}"
            try:
                list_response = ti.xcom_pull(task_ids=producer_task_id, key=xcom_key)
                if list_response:
                    all_batch_results[list_name] = list_response
                else:
                    logger.warning(f"No XCom data found for list '{list_name}' (key: {xcom_key}) from {producer_task_id}")
            except Exception as e:
                logger.error(f"Error pulling XCom for list '{list_name}' (key: {xcom_key}, task: {producer_task_id}): {e}", exc_info=True)
                errors_pulling_xcom += 1
    
    logger.info(f"XCom Pulling Summary: Found data for {len(all_batch_results)} lists. Encountered {errors_pulling_xcom} errors during XCom pull attempts.")
    
    for list_name in NYT_BESTSELLER_LISTS:
        response = all_batch_results.get(list_name)
        if not response:
            logger.warning(f"No response data to process for list: '{list_name}'. Skipping.")
            continue
        if not isinstance(response, dict):
            logger.warning(f"Response for list '{list_name}' is not a dict (type: {type(response)}). Skipping.")
            continue
            
        processed_lists += 1
        try:
            results_data = response.get("results", {})
            if not isinstance(results_data, dict):
                logger.warning(f"'results' field for '{list_name}' is not a dict (type: {type(results_data)}). Skipping.")
                continue
                
            books = results_data.get("books", [])
            if not isinstance(books, list):
                logger.warning(f"'books' field for '{list_name}' is not a list (type: {type(books)}). Skipping.")
                continue
                
            isbns_from_list = 0
            for book in books:
                if not isinstance(book, dict):
                    logger.warning(f"Book entry in '{list_name}' is not a dict (type: {type(book)}). Skipping.")
                    continue
                isbn = book.get("primary_isbn13") or book.get("primary_isbn10")
                if isbn:
                    isbn_clean = str(isbn).strip()
                    if isbn_clean and isbn_clean not in all_isbns:
                        all_isbns.add(isbn_clean)
                        isbns_from_list += 1
            logger.info(f"Extracted {isbns_from_list} new unique ISBNs from list '{list_name}'")
        except Exception as e:
            logger.error(f"Error processing books data for list '{list_name}': {e}", exc_info=True)
            continue
            
    unique_isbns = list(all_isbns)
    logger.info(f"ISBN Extraction Summary: Extracted {len(unique_isbns)} unique ISBNs from {processed_lists} NYT lists that had processable data.")
    if not unique_isbns:
        logger.warning("No unique ISBNs extracted.")
        
    ti.xcom_push(key="unique_isbns_list", value=unique_isbns)
    return unique_isbns

def create_isbn_batches(**kwargs):
    """Split ISBNs into batches for API processing."""
    ti = kwargs['ti']
    try:
        unique_isbns = ti.xcom_pull(task_ids="extract_unique_isbns", key="unique_isbns_list")

        if unique_isbns is None:
            logger.warning("'unique_isbns_list' XCom key returned None. Trying direct pull from 'extract_unique_isbns'.")
            unique_isbns = ti.xcom_pull(task_ids="extract_unique_isbns")

        if not unique_isbns:
            logger.warning("No ISBNs found to batch. Returning empty list.")
            ti.xcom_push(key="isbn_batch_configs", value=[])
            return []
            
        batches = [unique_isbns[i:i + ISBN_BATCH_SIZE] for i in range(0, len(unique_isbns), ISBN_BATCH_SIZE)]
        logger.info(f"Created {len(batches)} ISBN batches of size {ISBN_BATCH_SIZE}")
        
        batch_configs = []
        for i, batch in enumerate(batches):
            batch_configs.append({
                "isbn_batch": batch,
                "batch_index": i
            })
            
        ti.xcom_push(key="isbn_batch_configs", value=batch_configs)
        return batch_configs
    except Exception as e:
        logger.error(f"Critical error in create_isbn_batches: {e}", exc_info=True)
        ti.xcom_push(key="isbn_batch_configs", value=[])
        return []

def map_batches_to_inputs(**kwargs):
    """Map ISBN batches to the format needed for batch preparation."""
    ti = kwargs['ti']
    try:
        batch_configs = ti.xcom_pull(task_ids="create_isbn_batches", key="isbn_batch_configs")

        if batch_configs is None:
            logger.warning("'isbn_batch_configs' XCom key returned None. Trying direct pull from 'create_isbn_batches'.")
            batch_configs = ti.xcom_pull(task_ids="create_isbn_batches")
            
        if not batch_configs:
            logger.warning("No batch configs found to map. Returning empty list.")
            return [] 
        
        batch_inputs = []
        for config in batch_configs:
            if isinstance(config, dict) and "isbn_batch" in config:
                batch_inputs.append({"batch_input": config})
            else:
                logger.warning(f"Invalid batch config item found: {str(config)[:200]}. Skipping.")
        return batch_inputs
    except Exception as e:
        logger.error(f"Critical error in map_batches_to_inputs: {e}", exc_info=True)
        return []

def prepare_isbn_batch(batch_input, **kwargs):
    """Prepare a single ISBN batch for API processing."""
    batch_index = batch_input.get('batch_index', 'unknown')
    try:
        isbn_batch = batch_input.get("isbn_batch", [])
        if not isbn_batch:
            logger.warning(f"ISBN batch for index {batch_index} is empty.")
            payload = {"isbns": ""}
        else:
            isbn_str = ",".join(map(str, isbn_batch))
            payload = {"isbns": isbn_str}
            logger.info(f"Prepared {len(isbn_batch)} ISBNs for batch {batch_index}")
        return {
            "batch_data": {
                "payload": payload,
                "batch_index": batch_index,
                "isbn_count": len(isbn_batch)
            }
        }
    except Exception as e:
        logger.error(f"Error in prepare_isbn_batch for batch_input {str(batch_input)[:200]}: {e}", exc_info=True)
        return {
            "batch_data": {
                "payload": {"isbns": ""},
                "batch_index": batch_input.get("batch_index", -1),
                "isbn_count": 0
            }
        }

def process_isbn_batch(**kwargs):
    """Process a batch of ISBNs with a specific API."""
    batch_data = kwargs.get("batch_data", {})
    function_url = kwargs.get("function_url")
    source_name = kwargs.get("source_name")
    batch_index = batch_data.get("batch_index", -1)
    payload = batch_data.get("payload", {})
    isbn_count = batch_data.get("isbn_count", 0)
    log_prefix = f"[{source_name} Batch {batch_index + 1}]"
    
    if not function_url:
        logger.error(f"{log_prefix} Function URL is missing")
        return {
            "status": "error", 
            "reason": "missing_url", 
            "batch_index": batch_index,
            "items_returned_by_cf": 0
        }
    if not isbn_count:
        logger.warning(f"{log_prefix} Batch is empty, skipping API call")
        return {
            "status": "skipped", 
            "reason": "empty_batch", 
            "batch_index": batch_index,
            "items_returned_by_cf": 0
        }
    try:
        cf_response = call_api(function_url, payload)
        
        items_returned_count = 0
        if isinstance(cf_response, list):
            items_returned_count = len(cf_response)
        elif cf_response:
            items_returned_count = 1

        logger.info(f"{log_prefix} API call successful, Cloud Function returned {items_returned_count} items/entries.")
        return {
            "status": "success",
            "source": source_name.lower(),
            "batch_index": batch_index,
            "items_returned_by_cf": items_returned_count
        }
    except Exception as e:
        logger.error(f"{log_prefix} Error calling API: {e}", exc_info=True)
        return {
            "status": "error",
            "source": source_name.lower(),
            "batch_index": batch_index,
            "error": str(e),
            "items_returned_by_cf": 0
        }

def prepare_all_batches(source_name, **kwargs):
    """Prepare all ISBN batches for a specific API."""
    ti = kwargs['ti']
    try:
        if source_name == "OpenLibrary":
            map_task_id = "open_library_processing.map_ol_batches"
        elif source_name == "GoogleBooks":
            map_task_id = "google_books_processing.map_gb_batches"
        else:
            logger.error(f"Invalid source_name '{source_name}' in prepare_all_batches")
            return []
            
        batch_inputs_list = ti.xcom_pull(task_ids=map_task_id)

        if not batch_inputs_list:
            logger.warning(f"No batch inputs found from {map_task_id} for {source_name}. Returning empty list.")
            return []
            
        prepared_batches_data = []
        for item_dict in batch_inputs_list:
            if not isinstance(item_dict, dict):
                logger.warning(f"Expected a dict in batch_inputs_list, got {type(item_dict)}. Skipping.")
                continue
            batch_input_config = item_dict.get("batch_input")
            if not batch_input_config:
                logger.warning(f"'batch_input' key missing or empty in item_dict. Skipping.")
                continue
            try:
                result = prepare_isbn_batch(batch_input=batch_input_config)
                if result and "batch_data" in result:
                    prepared_batches_data.append(result["batch_data"])
                    logger.info(f"Successfully prepared batch {batch_input_config.get('batch_index', 'unknown')} for {source_name}")
                else:
                    logger.warning(f"Failed to prepare batch {batch_input_config.get('batch_index', 'unknown')} for {source_name}")
            except Exception as e:
                logger.error(f"Error preparing individual batch {batch_input_config.get('batch_index', 'unknown')} for {source_name}: {e}", exc_info=True)
                
        logger.info(f"Prepared {len(prepared_batches_data)} batches of data for {source_name}")
        return prepared_batches_data
    except Exception as e:
        logger.error(f"Critical error in prepare_all_batches for {source_name}: {e}", exc_info=True)
        return []

def process_all_batches(source_name, **kwargs):
    """Process all prepared batches with a specific API."""
    ti = kwargs['ti']
    try:
        if source_name == "OpenLibrary":
            prepare_task_id = "open_library_processing.prepare_ol_batches"
            url_task_id = "get_ol_function_url"
            url_key = "ol_function_url"
        elif source_name == "GoogleBooks":
            prepare_task_id = "google_books_processing.prepare_gb_batches"
            url_task_id = "get_gb_function_url"
            url_key = "gb_function_url"
        else:
            logger.error(f"Invalid source_name '{source_name}' in process_all_batches")
            return []

        prepared_batches_list = ti.xcom_pull(task_ids=prepare_task_id)
        function_url = ti.xcom_pull(task_ids=url_task_id, key=url_key)
        
        if not prepared_batches_list:
            logger.warning(f"No prepared batches found from {prepare_task_id} for {source_name}. Returning empty list.")
            return []
        if not function_url:
            logger.error(f"No function URL found for {source_name}. Cannot process batches.")
            return []
            
        results = []
        for batch_data_item in prepared_batches_list:
            if not isinstance(batch_data_item, dict):
                logger.warning(f"Expected a dict in prepared_batches_list, got {type(batch_data_item)}. Skipping.")
                continue
            try:
                batch_idx = batch_data_item.get('batch_index', 'unknown')
                result = process_isbn_batch(
                    batch_data=batch_data_item,
                    function_url=function_url,
                    source_name=source_name
                )
                results.append(result)
                logger.info(f"Processed batch {batch_idx} with status: {result.get('status') if isinstance(result, dict) else 'N/A'}")
            except Exception as e:
                current_batch_idx = batch_data_item.get("batch_index", -1) if isinstance(batch_data_item, dict) else -1
                logger.error(f"Error processing individual batch {current_batch_idx} for {source_name}: {e}", exc_info=True)
                results.append({
                    "status": "error", "source": source_name.lower(),
                    "batch_index": current_batch_idx, "error": str(e)
                })
                
        successes = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
        errors = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "error")
        logger.info(f"{source_name} Processing Summary: {successes} successes, {errors} errors.")
        return results
    except Exception as e:
        logger.error(f"Critical error in process_all_batches for {source_name}: {e}", exc_info=True)
        return []

# --- DAG Definition ---
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=["book_data", "data_ingestion"],
) as dag:

    # -- URL Resolution Tasks --
    get_nyt_function_url = PythonOperator(
        task_id="get_nyt_function_url",
        python_callable=fetch_and_store_url,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "location": GCP_LOCATION,
            "function_name": NYT_FUNCTION_NAME,
            "xcom_key": "nyt_function_url"
        },
        provide_context=True 
    )
    
    get_ol_function_url = PythonOperator(
        task_id="get_ol_function_url",
        python_callable=fetch_and_store_url,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "location": GCP_LOCATION,
            "function_name": OL_FUNCTION_NAME,
            "xcom_key": "ol_function_url"
        },
        provide_context=True 
    )
    
    get_gb_function_url = PythonOperator(
        task_id="get_gb_function_url",
        python_callable=fetch_and_store_url,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "location": GCP_LOCATION,
            "function_name": GB_FUNCTION_NAME,
            "xcom_key": "gb_function_url"
        },
        provide_context=True 
    )
    
    # -- NYT API Processing --
    with TaskGroup(group_id="nyt_api_calls") as nyt_api_calls:
        previous_batch_group = None
        
        for i, batch_of_lists in enumerate(NYT_BESTSELLER_BATCHES):
            with TaskGroup(group_id=f"nyt_batch_{i}") as current_batch_group:
                process_nyt_batch_task = PythonOperator(
                    task_id=f"process_nyt_batch", # This task_id is referred to in extract_unique_isbns
                    python_callable=process_nyt_batch,
                    op_kwargs={
                        "batch_idx": i,
                        "lists": batch_of_lists
                    },
                    provide_context=True
                )
                get_nyt_function_url >> process_nyt_batch_task
            
            if previous_batch_group:
                wait_task = PythonOperator(
                    task_id=f"wait_after_batch_{i-1}",
                    python_callable=wait_between_batches,
                    op_kwargs={"batch_index": i-1},
                    provide_context=True
                )
                previous_batch_group >> wait_task >> current_batch_group
            previous_batch_group = current_batch_group
    
    # -- ISBN Extraction and Batching --
    extract_unique_isbns_task = PythonOperator(
        task_id="extract_unique_isbns",
        python_callable=extract_unique_isbns,
        provide_context=True
    )
    
    create_isbn_batches_task = PythonOperator(
        task_id="create_isbn_batches",
        python_callable=create_isbn_batches,
        provide_context=True
    )
    
    # -- Process ISBNs with Open Library API --
    with TaskGroup(group_id="open_library_processing") as open_library_processing:
        map_ol_batches_task = PythonOperator(
            task_id="map_ol_batches", 
            python_callable=map_batches_to_inputs,
            provide_context=True
        )
        
        prepare_ol_batches_task = PythonOperator(
            task_id="prepare_ol_batches", 
            python_callable=prepare_all_batches,
            op_kwargs={"source_name": "OpenLibrary"},
            provide_context=True
        )
        
        process_ol_batches_task = PythonOperator(
            task_id="process_ol_batches",
            python_callable=process_all_batches,
            op_kwargs={"source_name": "OpenLibrary"},
            provide_context=True
        )
        map_ol_batches_task >> prepare_ol_batches_task >> process_ol_batches_task
    
    # -- Process ISBNs with Google Books API --
    with TaskGroup(group_id="google_books_processing") as google_books_processing:
        map_gb_batches_task = PythonOperator(
            task_id="map_gb_batches", 
            python_callable=map_batches_to_inputs,
            provide_context=True
        )
        
        prepare_gb_batches_task = PythonOperator(
            task_id="prepare_gb_batches", 
            python_callable=prepare_all_batches,
            op_kwargs={"source_name": "GoogleBooks"},
            provide_context=True
        )
        
        process_gb_batches_task = PythonOperator(
            task_id="process_gb_batches",
            python_callable=process_all_batches,
            op_kwargs={"source_name": "GoogleBooks"},
            provide_context=True
        )
        map_gb_batches_task >> prepare_gb_batches_task >> process_gb_batches_task
    
    # -- Set Overall DAG Dependencies --
    nyt_api_calls >> extract_unique_isbns_task
    extract_unique_isbns_task >> create_isbn_batches_task
    
    create_isbn_batches_task >> open_library_processing
    create_isbn_batches_task >> google_books_processing
    
    get_ol_function_url >> open_library_processing
    get_gb_function_url >> google_books_processing