"""
Google Trends API connector for retrieving search interest data.
Uses the pytrends library with caching to retrieve interest over time, by region, and related queries.
"""

import os
import json
import pandas as pd
from datetime import datetime,timedelta
import logging

# Import the cache
from src.utils.trends_cache import SimpleGoogleTrendsCache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoogleTrendsAPI:
    """
    Client for retrieving Google Trends data with caching.
    """

    def __init__(self, hl='en-US', tz=360, cache_dir="data/trends_cache",
                 cache_max_age_days=7, request_interval=60, retries=3, backoff_factor=2, session_cooldown=60):
        """
        Initialize the GoogleTrendsAPI client with caching.

        Args:
            hl (str, optional): Language for Google Trends
            tz (int, optional): Timezone offset
            cache_dir (str): Directory to store cached data
            cache_max_age_days (int): Maximum age of cached data in days
            request_interval (int): Minimum seconds between requests
            retries (int): Number of retry attempts for failed requests
            backoff_factor (int): Backoff factor for retries
            session_cooldown (int): Time to wait before creating a new session
        """
        self.cache = SimpleGoogleTrendsCache(
            cache_dir=cache_dir,
            max_age_days=cache_max_age_days,
            request_interval=request_interval
        )

        # Store these for reference but they're used by the cache
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.session_cooldown = session_cooldown

        logger.info("Initialized GoogleTrendsAPI with caching")

    def get_interest_over_time(self, keywords, timeframe='now 7-d'):
        """Get interest over time data for keywords."""
        # [UPDATED] Validate and normalize input
        if isinstance(keywords, str):
            keywords = [keywords]

        # [UPDATED] Filter out empty or non-string keywords
        valid_keywords = [k for k in keywords if k and isinstance(k, str) and k.strip()]
        if not valid_keywords:
            logger.warning("No valid keywords provided to get_interest_over_time")
            return pd.DataFrame()

        # [UPDATED] Ensure timeframe is a string
        if not isinstance(timeframe, str):
            logger.warning(f"timeframe must be a string, got {type(timeframe)}. Using default.")
            timeframe = 'today 1-m'

        logger.info(f"Getting interest over time for: {valid_keywords}")
        return self.cache.get_interest_over_time(valid_keywords, timeframe)

    def get_interest_by_region(self, keywords, resolution='REGION', timeframe='now 7-d'):
        """
        Get interest by region data for keywords.

        Args:
            keywords (list): List of search terms (max 5)
            resolution (str, optional): Geographic level (COUNTRY, REGION, CITY, DMA)
            timeframe (str, optional): Time period

        Returns:
            pandas.DataFrame: Interest by region data
        """
        return self.cache.get_interest_by_region(keywords, resolution, timeframe)

    def get_related_queries(self, keywords, timeframe='now 7-d'):
        """
        Get related queries data for keywords.

        Args:
            keywords (list): List of search terms (max 5)
            timeframe (str, optional): Time period

        Returns:
            dict: Related queries data
        """
        return self.cache.get_related_queries(keywords, timeframe)

    def get_trending_searches(self, country='united_states'):
        """
        Get daily trending searches.
        Note: This method doesn't use caching since the data changes daily.

        Args:
            country (str, optional): Country code

        Returns:
            pandas.DataFrame: Trending searches
        """
        logger.warning("get_trending_searches doesn't use caching - may be rate limited")
        try:
            # Request directly from the cache's pytrends object
            self.cache._throttle_request()  # Respect the rate limit
            data = self.cache.pytrends.trending_searches(pn=country)
            return data
        except Exception as e:
            logger.error(f"Error retrieving trending searches: {str(e)}")
            return pd.DataFrame()

    def get_interest_for_team_event(self, team_name, event_date, timeframe=None):
        """Get interest data for a specific team around an event date."""
        # [UPDATED] Input validation
        if not team_name or not isinstance(team_name, str):
            logger.warning(f"Invalid team name: {team_name}")
            return {"error": "Invalid team name"}

        if not event_date:
            logger.warning("No event date provided")
            # Use a reasonable default timeframe
            timeframe = 'today 1-m'
        elif not timeframe:
            # Try to create a 2-week window around the event
            try:
                event_date_obj = datetime.strptime(event_date, '%Y-%m-%d')
                # Use a shorter timeframe - 5 days before and after
                start_date = (event_date_obj - timedelta(days=5)).strftime('%Y-%m-%d')
                end_date = (event_date_obj + timedelta(days=5)).strftime('%Y-%m-%d')
                timeframe = f"{start_date} {end_date}"
            except (ValueError, TypeError):
                logger.warning(f"Invalid event date format: {event_date}")
                timeframe = 'today 1-m'  # Fallback

        # [UPDATED] Ensure timeframe is a string
        if not isinstance(timeframe, str):
            logger.warning(f"timeframe must be a string, got {type(timeframe)}. Using default.")
            timeframe = 'today 1-m'

        logger.info(f"Getting team event interest for: {team_name}, date: {event_date}, timeframe: {timeframe}")

        # Get interest over time with caching
        try:
            interest_over_time = self.get_interest_over_time([team_name], timeframe)
        except Exception as e:
            logger.error(f"Error getting interest over time: {str(e)}")
            interest_over_time = pd.DataFrame()

        # Get interest by region with caching (use a simpler timeframe for this)
        try:
            interest_by_region = self.get_interest_by_region([team_name], timeframe='today 1-m')
        except Exception as e:
            logger.error(f"Error getting interest by region: {str(e)}")
            interest_by_region = pd.DataFrame()

        # Get related queries with caching (use a simpler timeframe for this)
        try:
            related_queries = self.get_related_queries([team_name], timeframe='today 1-m')
        except Exception as e:
            logger.error(f"Error getting related queries: {str(e)}")
            related_queries = {}

        result = {
            'team': team_name,
            'event_date': event_date,
            'timeframe': timeframe,
            'interest_over_time': interest_over_time,
            'interest_by_region': interest_by_region,
            'related_queries': related_queries
        }

        return result