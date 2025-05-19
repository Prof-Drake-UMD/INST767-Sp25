import json
from airflow.exceptions import AirflowFailException, AirflowSkipException
import logging
from urllib.parse import unquote
#from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



class ExtractNdcChunk():
    """
    Description:
    A class to fetch drug event data from the OpenFDA API and store it in storage.
    """


    def __init__(self, chunk_id):
        self.chunk_id = chunk_id

    
    def fetch_ndc_chunk(self, url, api_key, record_limit) -> dict:
        """
        Description:
        Fetch a chunk of ndc data from the API based on the providedchunk ID.
        
        Input Parameters:
        url (str): The base URL of the OpenFDA API.
        
        Output Parameters:
        dict: A dictionary containing:
            data (str): The retrieved data in JSON format.
        """
        import requests

        if self.chunk_id is None:
            raise AirflowFailException(f"No data extracted from {url}")
        
        params = {
            "search": f"finished:true AND marketing_start_date:[2019-01-01 TO 2024-12-31]",
            "limit": record_limit,
            "skip": self.chunk_id * record_limit,
            "api_key": api_key
        }

        response = requests.get(url, params=params)

        logger.info(f'Fetching from URL: {unquote(response.url)}')

        json_response = response.json()

        if 'results' not in json_response or not json_response['results']:
            logger.warning(f"No results found for chunk {self.chunk_id}, skipping task.")
            raise AirflowSkipException(f"No results found for chunk {self.chunk_id}")

        json_data = json.dumps(json_response['results'])

        return json_data
        #return json_data
    



# ndc_directory_extractor = ExtractNdcChunk(0)

# ndc_directory_data = ndc_directory_extractor.fetch_ndc_chunk('https://api.fda.gov/drug/ndc.json', 'NebaLf3RBFwixQatBtfqnCcCa7M7apfggKciPMBO' , 1)

# print(ndc_directory_data)
