"""
Simple caching system for Google Trends data to avoid rate limiting.
"""

import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from pytrends.request import TrendReq
import logging

# Get logger
logger = logging.getLogger(__name__)


class SimpleGoogleTrendsCache:
    """Simple cache for Google Trends data with request throttling."""

    def __init__(self, cache_dir="data/trends_cache", max_age_days=7, request_interval=60):
        """
        Initialize the cache.

        Args:
            cache_dir: Directory to store cached data
            max_age_days: Maximum age of cached data in days
            request_interval: Minimum seconds between requests
        """
        self.cache_dir = cache_dir
        self.max_age_days = max_age_days
        self.request_interval = request_interval
        self.last_request_time = 0

        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)

        # Initialize pytrends
        self.pytrends = TrendReq(hl='en-US', tz=360)
        logger.info(f"Initialized Google Trends cache with interval {request_interval}s")

    def _get_cache_path(self, keywords, timeframe, method="interest_over_time"):
        """Get the cache file path for the query."""
        # Handle both string and list keywords
        if isinstance(keywords, list):
            keywords_str = "_".join([k.replace(" ", "-") for k in keywords])
        else:
            keywords_str = keywords.replace(" ", "-")

        # Limit filename length
        if len(keywords_str) > 100:
            keywords_str = keywords_str[:100]

        # Handle timeframe
        if isinstance(timeframe, str):
            timeframe_str = timeframe.replace(" ", "_").replace("/", "-")
        else:
            timeframe_str = "default"

        return os.path.join(self.cache_dir, f"{method}_{keywords_str}_{timeframe_str}.json")


    def _is_cache_valid(self, cache_path):
        """Check if cache file exists and is not too old."""
        if not os.path.exists(cache_path):
            return False

        # Check age of cache file
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
        max_age = datetime.now() - timedelta(days=self.max_age_days)
        return file_time > max_age

    def _throttle_request(self):
        """Wait if necessary to maintain minimum interval between requests."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        if elapsed < self.request_interval:
            wait_time = self.request_interval - elapsed
            logger.info(f"Throttling request, waiting {wait_time:.1f} seconds...")
            time.sleep(wait_time)

        self.last_request_time = time.time()

    def _is_valid_date_range(self, timeframe):
        """Check if a timeframe string is a valid date range (YYYY-MM-DD YYYY-MM-DD)."""
        if not timeframe or not isinstance(timeframe, str):
            return False

        # Check if it's already a list before splitting
        if isinstance(timeframe, list):
            # Cannot be a valid date range if it's a list
            return False

        parts = timeframe.split()
        if len(parts) != 2:
            return False

        try:
            start, end = parts
            datetime.strptime(start, '%Y-%m-%d')
            datetime.strptime(end, '%Y-%m-%d')
            return True
        except ValueError:
            return False



    def get_interest_over_time(self, keywords, timeframe="today 3-m"):
        """Get interest over time data, using cache if available."""
        if not keywords:
            logger.error("No keywords provided")
            return pd.DataFrame()

        # Normalize keywords
        if isinstance(keywords, str):
            keywords = [keywords]

        # Limit to 5 keywords (Google Trends limit)
        keywords = keywords[:5]

        # Filter out empty keywords and normalize
        keywords = [k.strip() for k in keywords if k and isinstance(k, str) and k.strip()]
        if not keywords:
            logger.error("No valid keywords after filtering")
            return pd.DataFrame()

        # Validate timeframe
        if not isinstance(timeframe, str):
            logger.warning(f"timeframe must be a string, got {type(timeframe)}. Using default.")
            timeframe = 'today 3-m'

        valid_timeframes = ['now 1-H', 'now 4-H', 'now 1-d', 'now 7-d', 'today 1-m', 'today 3-m', 'today 12-m',
                            'today 5-y']
        if timeframe not in valid_timeframes and not self._is_valid_date_range(timeframe):
            logger.warning(f"Potentially invalid timeframe: {timeframe}, using fallback")
            timeframe = 'today 3-m'  # Fallback to a safe default

        cache_path = self._get_cache_path(keywords, timeframe, "interest_over_time")

        # Return cached data if valid
        if self._is_cache_valid(cache_path):
            logger.info(f"Using cached data for {keywords}")
            try:
                with open(cache_path, 'r') as f:
                    json_data = json.load(f)
                    df = pd.DataFrame(json_data)
                    # Convert date column back to datetime index
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                        df.set_index('date', inplace=True)
                    return df
            except Exception as e:
                logger.error(f"Error reading cache file: {e}")
                # If we can't read the cache, continue to fetch fresh data

        # Throttle request if needed
        self._throttle_request()

        try:
            # Make the API request
            logger.info(f"Fetching fresh data for {keywords}")
            self.pytrends.build_payload(keywords, timeframe=timeframe)
            data = self.pytrends.interest_over_time()

            # Save to cache if successful
            if not data.empty:
                # Reset index to include date as a column
                df_to_save = data.reset_index()
                # Convert to records format
                json_data = df_to_save.to_json(orient='records', date_format='iso')
                with open(cache_path, 'w') as f:
                    f.write(json_data)
                logger.info(f"Saved data to cache: {cache_path}")

            return data


        except Exception as e:
            logger.error(f"Error in get_interest_over_time: {str(e)}")
            return pd.DataFrame()

    def get_interest_by_region(self, keywords, resolution='COUNTRY', timeframe='today 3-m'):
        """Get interest by region data, using cache if available."""
        if not keywords:
            logger.error("No keywords provided")
            return pd.DataFrame()

        if isinstance(keywords, str):
            keywords = [keywords]

        cache_path = self._get_cache_path(keywords, timeframe, f"interest_by_region_{resolution}")

        # Return cached data if valid
        if self._is_cache_valid(cache_path):
            logger.info(f"Using cached data for {keywords} by region")
            try:
                with open(cache_path, 'r') as f:
                    json_data = json.load(f)
                    return pd.DataFrame(json_data)
            except Exception as e:
                logger.error(f"Error reading cache file: {e}")

        # Throttle request if needed
        self._throttle_request()

        try:
            # Make the API request
            logger.info(f"Fetching fresh region data for {keywords}")
            self.pytrends.build_payload(keywords, timeframe=timeframe)
            data = self.pytrends.interest_by_region(resolution=resolution)

            # Save to cache if successful
            if not data.empty:
                # Reset index to include region as a column
                df_to_save = data.reset_index()
                # Convert to records format
                json_data = df_to_save.to_json(orient='records')
                with open(cache_path, 'w') as f:
                    f.write(json_data)
                logger.info(f"Saved region data to cache: {cache_path}")

            return data
        except Exception as e:
            logger.error(f"Error fetching region data: {e}")
            return pd.DataFrame()

    def get_related_queries(self, keywords, timeframe='today 3-m'):
        """Get related queries data, using cache if available."""
        if not keywords:
            logger.error("No keywords provided")
            return {}

        if isinstance(keywords, str):
            keywords = [keywords]

        cache_path = self._get_cache_path(keywords, timeframe, "related_queries")

        # Return cached data if valid
        if self._is_cache_valid(cache_path):
            logger.info(f"Using cached related queries for {keywords}")
            try:
                with open(cache_path, 'r') as f:
                    cached_data = json.load(f)
                    # Convert back to proper structure with DataFrames
                    result = {}
                    for keyword, data in cached_data.items():
                        result[keyword] = {}
                        for query_type, queries in data.items():
                            if queries is not None:
                                result[keyword][query_type] = pd.DataFrame(queries)
                            else:
                                result[keyword][query_type] = None
                    return result
            except Exception as e:
                logger.error(f"Error reading cache file: {e}")

        # Throttle request if needed
        self._throttle_request()

        try:
            # Make the API request
            logger.info(f"Fetching fresh related queries for {keywords}")
            self.pytrends.build_payload(keywords, timeframe=timeframe)
            data = self.pytrends.related_queries()

            # Save to cache if successful
            if data:
                # Convert to serializable format
                serializable_data = {}
                for keyword, keyword_data in data.items():
                    serializable_data[keyword] = {}
                    for query_type, df in keyword_data.items():
                        if df is not None and not df.empty:
                            serializable_data[keyword][query_type] = df.to_dict(orient='records')
                        else:
                            serializable_data[keyword][query_type] = None

                with open(cache_path, 'w') as f:
                    json.dump(serializable_data, f)
                logger.info(f"Saved related queries to cache: {cache_path}")

            return data
        except Exception as e:
            logger.error(f"Error fetching related queries: {e}")
            return {}