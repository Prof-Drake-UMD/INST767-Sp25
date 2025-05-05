"""
Make API calls and pull the data down into json files
"""
import os
import json
import logging
import time
from typing import Dict, List, Optional, Any
import requests

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AdzunaConnector:

    BASE_URL = "https://api.adzuna.com/v1/api/jobs"
    
    def __init__(self, app_id: str, app_key: str, country: str = "us",max_retries: int = 3, retry_delay: int = 5):
        self.app_id = app_id
        self.app_key = app_key
        self.country = country.lower()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
    def extract_jobs(self, 
                     keywords: Optional[List[str]] = None, 
                     results_per_page: int = 50,
                     max_pages: int = 10) -> List[Dict[str, Any]]:
        all_jobs = []
            
        
        for keyword in keywords:
            print(f"Extracting Adzuna jobs for keyword: {keyword}")
            page = 1
            
            while page <= max_pages:
                try:
                    jobs = self._fetch_jobs_page(keyword, page, results_per_page)
                    
                    if not jobs or not jobs.get('results'):
                        logger.info(f"No more results for keyword '{keyword}' after page {page-1}")
                        break
                    
                    all_jobs.extend(jobs.get('results', []))
                    logger.info(f"Extracted {len(jobs.get('results', []))} jobs from page {page} for keyword '{keyword}'")
                    
                    # Check if reach the end
                    if page >= jobs.get('count', 0) // results_per_page:
                        break
                        
                    page += 1
                except Exception as e:
                    logger.error(f"Error extracting jobs for keyword '{keyword}', page {page}: {str(e)}")
                    break
        
        logger.info(f"Total jobs extracted from Adzuna: {len(all_jobs)}")
        return all_jobs
    
    def _fetch_jobs_page(self, keyword: str, page: int, results_per_page: int) -> Dict[str, Any]:
        
        url = f"{self.BASE_URL}/{self.country}/search/{page}"
        
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": results_per_page,
            "what": keyword,
            "content-type": "application/json"
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

if __name__ == "__main__":
    # put api key in environment variable
    app_id = os.environ.get("ADZUNA_APP_ID")
    app_key = os.environ.get("ADZUNA_APP_KEY")
    keywords = ["software engineer", "data scientist", "devops engineer"]
    
    if app_id and app_key:
        connector = AdzunaConnector(app_id, app_key)
        jobs = connector.extract_jobs(keywords=keywords)
        os.makedirs("data", exist_ok=True)
        with open("data/adzuna_jobs.json", "w") as f:
            f.write(json.dumps(jobs, indent=2))
    else:
        logger.error("Missing required environment variables: ADZUNA_APP_ID, ADZUNA_APP_KEY")