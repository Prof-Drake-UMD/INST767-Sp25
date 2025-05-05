"""
Make API calls and pull the data down into json files
"""
import os
import json
import logging
import time
import http.client
from datetime import datetime
from typing import Dict, List, Optional, Any

#logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JoobleConnector:

    HOST = "jooble.org"
    
    def __init__(self, api_key: str, max_retries: int = 3, retry_delay: int = 5):
        self.api_key = api_key
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
    
    def extract_jobs(self, 
                     keywords: Optional[List[str]] = None, 
                     locations: Optional[List[str]] = None,
                     limit: int = 100) -> List[Dict[str, Any]]:
        
        all_jobs = []
        
        for keyword in keywords:
            for location in locations:
                try:
                    logger.info(f"Extracting Jooble jobs for keyword '{keyword}' in '{location}'")
                    jobs = self._fetch_jobs(keyword, location, limit)
                    
                    if jobs:
                        all_jobs.extend(jobs)
                        logger.info(f"Extracted {len(jobs)} jobs for keyword '{keyword}' in '{location}'")
                    else:
                        logger.warning(f"No jobs found for keyword '{keyword}' in '{location}'")
                    
                    time.sleep(1)  
                except Exception as e:
                    logger.error(f"Error extracting jobs for keyword '{keyword}' in '{location}': {str(e)}")
        
        logger.info(f"Total jobs extracted from Jooble: {len(all_jobs)}")
        return all_jobs
    
    def _fetch_jobs(self, keyword: str, location: str, limit: int) -> List[Dict[str, Any]]:
        
        payload = {
            "keywords": keyword,
            "location": location,
            "limit": limit
        }

        body = json.dumps(payload)
        headers = {"Content-type": "application/json"}
        
        for attempt in range(self.max_retries):
            try:
                connection = http.client.HTTPConnection(self.HOST)
                connection.request('POST', f'/api/{self.api_key}', body, headers)
                response = connection.getresponse()
                
                if response.status == 200:
                    response_data = response.read().decode('utf-8')
                    data = json.loads(response_data)
                    jobs = data.get("jobs", [])
                    connection.close()
                    return jobs
                else:
                    logger.warning(f"Request failed with status {response.status}: {response.reason}")
                    connection.close()
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    else:
                        raise Exception(f"Failed after {self.max_retries} attempts: {response.status} {response.reason}")
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

if __name__ == "__main__":
    # put api key in environment variable
    api_key = os.environ.get("JOOBLE_API_KEY")
    
    if api_key:
        connector = JoobleConnector(api_key)
        jobs = connector.extract_jobs(
            keywords=["software engineer", "ux"], 
            locations=["remote"], 
            limit=20
        )
        with open("data/jooble_jobs.json", "w") as f:
            json.dump(jobs, f, indent=4)
    else:
        logger.error("Missing Jooble API key. Set the JOOBLE_API_KEY environment variable.")
