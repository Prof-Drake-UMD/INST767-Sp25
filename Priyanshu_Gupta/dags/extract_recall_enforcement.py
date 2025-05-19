
import requests
import logging
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from datetime import datetime

from include.helpers.StorageClients import GCSClient
from include.extract_from_apis.ExtractRecallEnforcements import ExtractRecallEnforcementChunk
from include.helpers.PubSubHandler import PubSubHandler

# Define a global logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

project_id = 'inst767-openfda-pg'
topic_id = 'airflow-raw-event'

bucket_name = 'openfda-drug-events'
raw_prefix = 'recall_enforcement/raw'


GCSClient = GCSClient()
PubSubHandler = PubSubHandler(project_id)

api = BaseHook.get_connection('openfda_events_api')
api_key = api.extra_dejson["api_key"]



@dag(
    start_date = datetime(2024, 1, 1), 
    schedule = '@daily',
    catchup = True,
    tags = ['openFDA', 'recall_enforcements', 'pubsub', 'ETL']
)

def recall_enforcements_etl():
    """
    DAG for extracting drug event data from OpenFDA API, processing it in chunks,
    and storing the results efficiently.
    """
    
    @task.sensor(poke_interval=30,timeout=150, mode='poke')
    def poke_api( **kwargs) -> PokeReturnValue:
        """
        Description:
        Check if recall enforcement data is available for the execution date.
        
        Output Parameters:
        PokeReturnValue: Contains a flag indicating data availability and metadata if available.
        """

        
        # Construct the base URL
        #base_url = f"{api.conn_type}://{api.host}/{api.schema}"
        base_url = 'https://api.fda.gov/drug/enforcement.json'
        logger.info(f"Checking API availability at: {base_url}")
        params = {
         "search": f"openfda.product_ndc:[* TO *]"
        }

        response = requests.get(base_url, params=params)
        logger.info(f"Checking events availability for at: {response.url}")

        poke_results = {'base_url': base_url}

        try:
            total_records = response.json()['meta']['results']['total']
            poke_results['total_records'] = total_records
            return PokeReturnValue(is_done=total_records  > 0, xcom_value=poke_results)
        except KeyError as e:
            logger.error(f"No records found")
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
        Prepare parameters for each chunk, including API details and chunk id.
        
        Input Parameters:
        poke_results (dict): Contains API base URL.
        chunk_list (list): List of chunk IDs to process.
        
        Output Parameters:
        list: List of dictionaries with chunk-specific parameters.
        """
        base_url = poke_result["base_url"]
        return list({"chunk_id": chunk_id, "base_url": base_url} for chunk_id in chunk_list)
    

    @task
    def fetch_raw_and_send_to_pubsub(chunk_params):
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

        chunk = ExtractRecallEnforcementChunk(chunk_id)
        
        logger.info(f"Fetching chunk {chunk_id}")

        json_data = chunk.fetch_recall_enforcement_chunk(url=url, api_key=api_key, record_limit=1000)
        logger.info(f'Complete data fetching for chunk {chunk_id}.')

    
        object_name=f'{raw_prefix}/recall-enforcements-chunk-{chunk_id}.json'
        
        storage_path = GCSClient.store_data(data=json_data, bucket_name=bucket_name, object_name=object_name)

        logger.info(f'Stored chunk-{chunk_id} at {storage_path} ')

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        pubsub_message = {
                'table_name': 'recall-enforcements',
                "chunk_id": chunk_id,
                "storage_path": storage_path,
                "datetime": current_time
            }
        
        # pubsub_message_bytes = json.dumps(pubsub_message).encode("utf-8")

         # Publish to Pub/Sub
        future = PubSubHandler.publish_to_pubsub(topic_id,  pubsub_message)
        logger.info(f"Published message to Pub/Sub: {future.result()}")

        return pubsub_message
    

        
    poke_api_result = poke_api()
    chunks_count = calculate_chunks(poke_api_result, 1000)
    chunk_list = generate_chunk_ids(chunks_count)
    params = prepare_chunk_params(poke_api_result, chunk_list)
    pubsub_message = fetch_raw_and_send_to_pubsub.expand(chunk_params=params)


recall_enforcements_etl()
