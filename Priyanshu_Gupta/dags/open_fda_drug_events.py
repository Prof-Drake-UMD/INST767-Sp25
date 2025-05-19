import requests
import logging
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime

from include.helpers.LoadBigQuery import LoadBigQuery
from include.helpers.StorageClients import GCSClient
from include.openfdaAdverseEvents.extract_raw_events_chunk import ExtractEventChunk
from include.openfdaAdverseEvents.transform_events import transform_drug_events

from include.helpers.PubSubHandler import PubSubHandler

# Define a global logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

project_id = 'inst767-openfda-pg'
topic_id = 'open-fda-event-chunks'
bucket_name = 'openfda-drug-events'
raw_prefix = 'drugs-adverse-events/raw'
transformed_prefix = 'drugs-adverse-events/transformed'

project="inst767-openfda-pg"
dataset_id="openfda"
table_id = "drug-adverse-events"

GCSClient = GCSClient()
pubsub_handler = PubSubHandler(project_id)

api = BaseHook.get_connection('openfda_events_api')
api_key = api.extra_dejson["api_key"]


@dag(
    start_date = datetime(2024, 1, 1), 
    schedule = '@daily',
    catchup = True,
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
            #raise AirflowFailException(f"No records found for {execution_date_str}")
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
    
    

    @task
    def fetch_event_chunk_and_store(chunk_params):
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
        chunk = ExtractEventChunk(chunk_id, event_date)
        
        logger.info(f"Fetching chunk {chunk_id} for {event_date}")

        events_data = chunk.fetch_event_chunk(url=url, api_key=api_key, record_limit=1000)
        record_count = len(events_data)
        logger.info(f'Complete data fetching for chunk {chunk_id}.')

        date_partition = event_date.replace('-', '/')
        object_name=f'{raw_prefix}/{date_partition}/events-chunk-{chunk_id}.json'
        storage_path = GCSClient.store_data(data=events_data, bucket_name=bucket_name, object_name=object_name)

        logger.info(f'Stored chunk-{chunk_id} at {storage_path} ')

        return {
                "event_date": event_date,
                "chunk_id": chunk_id,
                "storage_path": storage_path
            }
        
        

    @task
    def transform_load_drug_events(raw_file_metadata):
        """Combine results from all chunks"""

        #read raw data to df
        event_date = raw_file_metadata[0]['event_date']
        raw_date_prefix = f"{raw_prefix}/{event_date.replace('-', '/')}"
        raw_df = GCSClient.read_raw_json_to_df(bucket_name, raw_date_prefix)

        #transform_data
        transformed_data = transform_drug_events(raw_df)
        transformed_file_name = event_date.replace('-', '')
        transformed_blob_path = f"{transformed_prefix}/{transformed_file_name}.json"

    #   Upload to GCS
        GCSClient.store_data(transformed_data, bucket_name, transformed_blob_path)
        # transformed_blob.upload_from_file(json_buffer, content_type='application/json')

        logger.info(f'Loaded transformed events to {transformed_blob_path}') 

        return transformed_blob_path

    
    @task
    def load_to_bq(transformed_blob_path):

        bq_table = f"{project}.{dataset_id}.{table_id}"

        #getting partition name from file name i.e 20240101.json
        partion_date = transformed_blob_path.split('/')[-1].replace('.json', '')

        BQClinet = LoadBigQuery(bucket_name, transformed_blob_path, bq_table)
        BQClinet.load_into_bq(partion_date)

        return bq_table

    # Define the workflow
    poke_api_result = poke_api()
    chunks_count = calculate_chunks(poke_api_result, 1000)
    chunk_list = generate_chunk_ids(chunks_count)
    params = prepare_chunk_params(poke_api_result, chunk_list)

    logger.info(f"Chunk_param: {params}")
    
    # Process each chunk using dynamic task mapping with task groups
    raw_file_metadata = fetch_event_chunk_and_store.expand(chunk_params=params)

    transformed_path = transform_load_drug_events(raw_file_metadata)

    load_to_bq(transformed_path)

open_fda_drug_events()


