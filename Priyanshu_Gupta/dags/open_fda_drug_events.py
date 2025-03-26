import requests
import logging
from airflow.decorators import dag, task, task_group
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.exceptions import AirflowSkipException
from datetime import datetime

from include.openFDA_events.ProcessEventsChunk import ProcessEventsChunk

# Define a global logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@dag(
    start_date = datetime(2024, 1, 1), 
    schedule = '@daily',
    catchup = False,
    tags = ['openFDA', 'drug_events', 'extraction']
)

def open_fda_drug_events():
    """
    DAG for extracting drug event data from OpenFDA API, processing it in chunks,
    and storing the results efficiently.
    """
    
    @task.sensor(poke_interval=30,timeout=150, mode='poke')
    def poke_api(execution_date, **kwargs) -> PokeReturnValue:
        """
        Description:
        Check if drug event data is available for the execution date.
        
        Input Parameters:
        execution_date (datetime): Execution date for the DAG.
        
        Output Parameters:
        PokeReturnValue: Contains a flag indicating data availability and metadata if available.
        """

        api = BaseHook.get_connection('openfda_events_api')
        # Construct the base URL
        base_url = f"{api.conn_type}://{api.host}/{api.schema}"
        logger.info(f"Checking API availability at: {base_url}")
        # date parameter to extract records for particular dates
        execution_date_str=execution_date.strftime('%Y-%m-%d')
        params = {
        "search": f"receivedate:{execution_date_str.replace('-', '')}"
        }

        response = requests.get(base_url, params=params)
        logger.info(f"Checking events availability for {execution_date_str} at: {response.url}")

        poke_results = {'base_url': base_url, 'event_date': execution_date_str}

        try:
            total_records = response.json()['meta']['results']['total']
            poke_results['total_records'] = total_records
            return PokeReturnValue(is_done=total_records  > 0, xcom_value=poke_results)
        except KeyError as e:
            logger.error(f"No records found for {execution_date_str}")
            poke_results['total_records'] = 0
            return PokeReturnValue(is_done=False, xcom_value=poke_results)

    @task
    def calculate_chunks(poke_results: dict, record_limit=1000) -> int: 
        """
        Description:
        Calculate the number of chunks required to process available records.
        
        Input Parameters:
        poke_results (dict): Dictionary containing total records available.
        record_limit (int): Maximum number of records per chunk (default: 1000).
        
        Output Parameters:
        int: Number of chunks needed.
        """

        import math
        total_records = poke_results['total_records']
        chunks = math.ceil(total_records / record_limit)
        return chunks
    
    @task
    def generate_chunk_ids(chunks: int) -> list:
        """
        Description:
        Generate a list of chunk IDs.
        
        Input Parameters:
        chunks (int): Number of chunks to generate.
        
        Output Parameters:
        list: List of chunk IDs.
        """
        return list(range(chunks))
    
    @task
    def prepare_chunk_params(poke_result: dict, chunk_list: list) -> list[dict[str, str]]:
        """
        Description:
        Prepare parameters for each chunk, including API details and event date.
        
        Input Parameters:
        poke_results (dict): Contains API base URL and event date.
        chunk_list (list): List of chunk IDs to process.
        
        Output Parameters:
        list: List of dictionaries with chunk-specific parameters.
        """
        base_url = poke_result["base_url"]
        event_date = poke_result["event_date"]
        return list({"chunk_id": chunk_id, "base_url": base_url, "event_date": event_date} for chunk_id in chunk_list)
    
    @task_group
    def process_single_chunk(chunk_params: dict):
        """
        Description:
        Task group to fetch and store event data for a single chunk.
        
        Input Parameters:
        params (dict): Chunk-specific parameters including chunk_id, base_url, and event_date.
        
        Output Parameters:
        dict: Chunk processing summary containing event date, chunk ID, records count, and storage path.
        """

        @task
        def fetch_event_chunk(chunk_params):
            """
            Description:
            Fetch event data for a specific chunk from OpenFDA API.
            
            Input Parameters:
            chunk_params (dict): Contains chunk_id, base_url, and event_date.
            
            Output Parameters:
            dict: Fetched json data from api.
            """
            chunk_id = chunk_params["chunk_id"]
            url = chunk_params["base_url"]
            event_date = chunk_params["event_date"]
            chunk = ProcessEventsChunk(chunk_id, event_date)
            
            logger.info(f"Fetching chunk {chunk_id} for {event_date}")

            fetch_result = chunk.fetch_event_chunk(url=url, record_limit=1000)
            logger.info(f'Complete data fetching for chunk {chunk_id}. Got {len(fetch_result)} events')
            return fetch_result
        
        

        @task
        def store_event_chunk(chunk_params, fetch_result):
            """
            Description:
            Store fetched event data into a predefined storage location.
            
            Input Parameters:
            fetch_result (dict): Contains chunk_id, event_date, and event_data.
            
            Output Parameters:
            str: Path where data was stored.
            """

            chunk_id = chunk_params["chunk_id"]
            event_date = chunk_params["event_date"]
            chunk = ProcessEventsChunk(chunk_id, event_date)

            events_data = fetch_result["events_data"]
            records_count = fetch_result['records_count']

            storage_path=chunk.store_event_chunk(storage_client='minio', events_data=events_data)
            logger.info(f'Stored chunk {chunk_id} with {records_count} records at {storage_path} ')

            return storage_path
        
        @task
        def final_chunk_results(chunk_params, fetched_data, storage_path):
            return {
                    "event_date": chunk_params["event_date"],
                    "chunk_id": chunk_params["chunk_id"],
                    "storage_path": storage_path,
                    "record_count": fetched_data['records_count']
            }
        

        #fetched_data = fetch_event_chunk(chunk_id, url, event_date)
        fetched_data = fetch_event_chunk(chunk_params)

        # Execute store operation
        storage_path = store_event_chunk(chunk_params, fetched_data)

        return final_chunk_results(chunk_params, fetched_data, storage_path)
        
        
        
       

        
    @task
    def combine_results(chunk_results):
        """Combine results from all chunks"""
        return chunk_results

    # Define the workflow
    poke_api_result = poke_api()
    chunks_count = calculate_chunks(poke_api_result)
    chunk_list = generate_chunk_ids(chunks_count)
    params = prepare_chunk_params(poke_api_result, chunk_list)

    logger.info(f"Chunk_param: {params}")
    
    # Process each chunk using dynamic task mapping with task groups
    chunk_results = process_single_chunk.expand(chunk_params=params)
    
    # Combine results from all chunks
    combine_results = combine_results(chunk_results)

open_fda_drug_events()


