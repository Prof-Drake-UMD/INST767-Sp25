import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class BaseAPIClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url
        self.api_key = api_key
        if api_key is not None and not api_key:
            raise ValueError("API key must be provided if specified")

        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _make_request(self, endpoint: str, params: dict = None, timeout: int = 20) -> tuple[Optional[dict], Optional[str]]:
        """
        Makes an HTTP request with error handling and logging.
        Returns a tuple of (response_data, error_message).
        If successful, error_message will be None.
        If failed, response_data will be None and error_message will contain the error.
        """
        response_text_for_error = None
        try:
            response = self.session.get(endpoint, params=params, timeout=timeout)
            response_text_for_error = response.text
            response.raise_for_status()
            return response.json(), None
        except requests.exceptions.Timeout as e:
            error_msg = f"Request timed out: {e}"
            logger.error(f"API Client: {error_msg}")
            return None, error_msg
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error {e.response.status_code if e.response else 'N/A'}: {response_text_for_error or (e.response.text if e.response else 'No response body')}"
            logger.error(f"API Client: {error_msg}")
            return None, error_msg
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {e}"
            logger.error(f"API Client: {error_msg}", exc_info=True)
            return None, error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logger.error(f"API Client: {error_msg}", exc_info=True)
            return None, error_msg 