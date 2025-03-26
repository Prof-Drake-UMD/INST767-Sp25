import json
from airflow.exceptions import AirflowFailException
from minio import Minio
from include.helpers.StorageClients import MinioClient
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ProcessEventsChunk:
    """
    Description:
    A class to fetch drug event data from the OpenFDA API and store it in storage.
    """
    def __init__(self, chunk_id, event_date):
        self.chunk_id = chunk_id
        self.event_date = event_date



    def fetch_event_chunk(self, url, record_limit) -> dict:
        """
        Description:
        Fetch a chunk of drug event data from the OpenFDA API based on the provided date and chunk ID.
        
        Input Parameters:
        url (str): The base URL of the OpenFDA API.
        date (str): The date for which to fetch event data (format: 'YYYY-MM-DD').
        
        Output Parameters:
        dict: A dictionary containing:
            - records_count (int): The number of records retrieved.
            - event_data (str): The retrieved event data in JSON format.
        """
        import requests

        if self.chunk_id is None:
            raise AirflowFailException(f"No data extracted from {url} for {self.event_date}")
        
        params = {
            "search": f"receivedate:{self.event_date.replace('-', '')}",
            "limit": record_limit,
            "skip": self.chunk_id * 1000
        }

        response = requests.get(url, params=params)
        logger.info(f'Fetching from URL: {response.url}')

        json_response = response.json()['results']
        records_count=len(json_response)
        json_data = json.dumps(json_response)
        

        return {"records_count":records_count, "events_data": json_data}


    def store_event_chunk(self, storage_client, events_data):
        """
        Description:
        Store the retrieved event chunk data in MinIO storage.
        
        Input Parameters:
        
        storage_client, 
        events_data: JSON string containing the event data, 
        
        Output Parameters:
        str: storage path where the chunk data is stored.
        """

        date=self.event_date.replace('-', '/')
        object_name=f'{date}/events-chunk-{self.chunk_id}.json'
        bucket_name='drug-events'
        events_data = json.loads(events_data)
        data = json.dumps(events_data, ensure_ascii=False).encode('utf8')

        if storage_client == 'minio':
            
            minio_client = MinioClient()

            storage_path = minio_client.store_data(data, bucket_name, object_name)

            logger.info(f"Storage Path: {storage_path}")
            
            return storage_path
        else:
            raise AirflowFailException(f"Storage client {storage_client} not set up in Airflow")


