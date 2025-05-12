import os
import json
import time
from typing import Dict, List, Optional, Any
import requests

class MuseConnector:
    
    BASE_URL = "https://www.themuse.com/api/public/jobs"
    API_VERSION = "v2"
    
    def __init__(self, api_key: str, max_retries: int = 3, retry_delay: int = 5):
        self.api_key = api_key
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
    def extract_jobs(self, 
                     categories: Optional[List[str]] = None, 
                     page_count: int = 20,
                     job_count_per_page: int = 20) -> List[Dict[str, Any]]:
        all_jobs = []
        
        for category in categories:
            for page in range(1, page_count + 1):
                try:
                    jobs = self._fetch_jobs_page(category, page, job_count_per_page)
                    
                    if not jobs or not jobs.get('results'):
                        print(f"No more results for category {category} after page {page-1}")
                        break
                    all_jobs.extend(jobs.get('results',[]))
                    print(f"Extracted {len(jobs)} jobs from page {page} for category '{category}'")
                except Exception as e:
                    print(f"Error extracting jobs for category '{category}', page {page}: {str(e)}")
        
        print(f"Total jobs extracted from The Muse: {len(all_jobs)}")
        return all_jobs
    
    def _fetch_jobs_page(self, category: str, page: int, count: int) -> List[Dict[str, Any]]:
        
        url = f"{self.BASE_URL}"
        
        params = {
            "api_key": self.api_key,
            "categories": category,
            "page": page,
            "page_size": count
        }
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1}/{self.max_retries} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

if __name__ == "__main__":
    api_key = os.environ.get("MUSE_API_KEY")
    categories = ["ux", "product management", "project management", "software engineer"]
    
    if api_key:
        connector = MuseConnector(api_key)
        jobs = connector.extract_jobs(categories = categories, page_count=1, job_count_per_page=5)
        with open("data/muse_jobs.json", "w") as f:
            f.write(json.dumps(jobs, indent=2))
    else:
        print("Missing required environment variables: MUSE_API_KEY")