import json
from airflow.exceptions import AirflowFailException
from minio import Minio
from include.helpers.StorageClients import MinioClient, GCSClient
#from include.helpers.PubSubHandler import PubSubHandler
import logging
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExtractEventChunk:
    """
    Description:
    A class to fetch drug event data from the OpenFDA API and store it in storage.
    """


    def __init__(self, chunk_id, event_date):
        self.chunk_id = chunk_id
        self.event_date = event_date



    def fetch_event_chunk(self, url, api_key, record_limit) -> dict:
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
            "skip": self.chunk_id * record_limit,
            "api_key": api_key
        }

        response = requests.get(url, params=params)
        logger.info(f'Fetching from URL: {response.url}')

        json_response = response.json()['results']
        records_count=len(json_response)
        json_data = json.dumps(json_response)
        
        return json_data


