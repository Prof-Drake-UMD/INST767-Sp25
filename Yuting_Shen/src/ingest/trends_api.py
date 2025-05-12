"""
Google Trends API connector for retrieving search interest data.
Uses the pytrends library to retrieve interest over time, by region, and related queries.
"""

import os
import json
import pandas as pd
from datetime import datetime
import logging
from pytrends.request import TrendReq
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoogleTrendsAPI:
    """
    Client for retrieving Google Trends data.
    """

    def __init__(self, hl='en-US', tz=360):
        """
        Initialize the GoogleTrendsAPI client.

        Args:
            hl (str, optional): Language for Google Trends
            tz (int, optional): Timezone offset
        """
        self.pytrends = TrendReq(hl=hl, tz=tz)

    def get_interest_over_time(self, keywords, timeframe='now 7-d'):
        """
        Get interest over time data for keywords.

        Args:
            keywords (list): List of search terms (max 5)
            timeframe (str, optional): Time period (e.g., 'now 1-d', 'now 7-d', 'today 3-m')

        Returns:
            pandas.DataFrame: Interest over time data
        """
        try:
            # Build the payload
            self.pytrends.build_payload(keywords, cat=0, timeframe=timeframe)

            # Get the data
            data = self.pytrends.interest_over_time()

            # Save raw data
            self._save_raw_data(data, f"interest_over_time_{'_'.join(keywords)[:50]}")

            logger.info(f"Retrieved interest over time data for {keywords}")
            return data

        except Exception as e:
            logger.error(f"Error retrieving interest over time: {str(e)}")
            # Sleep to avoid rate limiting
            time.sleep(5)
            return pd.DataFrame()

    def get_interest_by_region(self, keywords, resolution='COUNTRY', timeframe='now 7-d'):
        """
        Get interest by region data for keywords.

        Args:
            keywords (list): List of search terms (max 5)
            resolution (str, optional): Geographic level (COUNTRY, REGION, CITY, DMA)
            timeframe (str, optional): Time period

        Returns:
            pandas.DataFrame: Interest by region data
        """
        try:
            # Build the payload
            self.pytrends.build_payload(keywords, cat=0, timeframe=timeframe)

            # Get the data
            data = self.pytrends.interest_by_region(resolution=resolution)

            # Save raw data
            self._save_raw_data(data, f"interest_by_region_{'_'.join(keywords)[:50]}")

            logger.info(f"Retrieved interest by region data for {keywords}")
            return data

        except Exception as e:
            logger.error(f"Error retrieving interest by region: {str(e)}")
            # Sleep to avoid rate limiting
            time.sleep(5)
            return pd.DataFrame()

    def get_related_queries(self, keywords, timeframe='now 7-d'):
        """
        Get related queries data for keywords.

        Args:
            keywords (list): List of search terms (max 5)
            timeframe (str, optional): Time period

        Returns:
            dict: Related queries data
        """
        try:
            # Build the payload
            self.pytrends.build_payload(keywords, cat=0, timeframe=timeframe)

            # Get the data
            data = self.pytrends.related_queries()

            # Save raw data
            self._save_raw_data(data, f"related_queries_{'_'.join(keywords)[:50]}")

            logger.info(f"Retrieved related queries data for {keywords}")
            return data

        except Exception as e:
            logger.error(f"Error retrieving related queries: {str(e)}")
            # Sleep to avoid rate limiting
            time.sleep(5)
            return {}

    def get_trending_searches(self, country='united_states'):
        """
        Get daily trending searches.

        Args:
            country (str, optional): Country code

        Returns:
            pandas.DataFrame: Trending searches
        """
        try:
            # Get the data
            data = self.pytrends.trending_searches(pn=country)

            # Save raw data
            self._save_raw_data(data, f"trending_searches_{country}")

            logger.info(f"Retrieved trending searches for {country}")
            return data

        except Exception as e:
            logger.error(f"Error retrieving trending searches: {str(e)}")
            # Sleep to avoid rate limiting
            time.sleep(5)
            return pd.DataFrame()

    def get_interest_for_team_event(self, team_name, event_date, timeframe=None):
        """
        Get interest data for a specific team around an event date.

        Args:
            team_name (str): Name of the team
            event_date (str): Date of the event (YYYY-MM-DD)
            timeframe (str, optional): Custom timeframe override

        Returns:
            dict: Interest data for the team
        """
        # Default to a 2-week window around the event
        if not timeframe:
            # Create a timeframe of 1 week before and 1 week after the event
            timeframe = f"{event_date} {event_date}"

        # Get interest over time
        interest_over_time = self.get_interest_over_time([team_name], timeframe)

        # Get interest by region
        interest_by_region = self.get_interest_by_region([team_name], timeframe=timeframe)

        # Get related queries
        related_queries = self.get_related_queries([team_name], timeframe)

        result = {
            'team': team_name,
            'event_date': event_date,
            'timeframe': timeframe,
            'interest_over_time': interest_over_time,
            'interest_by_region': interest_by_region,
            'related_queries': related_queries
        }

        # Save the combined result
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/raw/trends_event_analysis_{team_name}_{event_date}_{timestamp}.json"

        # Save only the metadata since DataFrames cannot be directly serialized
        result_meta = {
            'team': team_name,
            'event_date': event_date,
            'timeframe': timeframe,
            'has_interest_over_time': not interest_over_time.empty,
            'has_interest_by_region': not interest_by_region.empty,
            'has_related_queries': bool(related_queries)
        }

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Write metadata to file
        with open(filepath, "w") as f:
            json.dump(result_meta, f, indent=2)

        logger.info(f"Saved event analysis metadata to {filepath}")

        return result

    def _save_raw_data(self, data, prefix):
        """
        Save raw data to a file with timestamp.

        Args:
            data: Data to save (DataFrame or dict)
            prefix (str): Prefix for the filename
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/raw/trends_{prefix}_{timestamp}.json"

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Convert DataFrame to JSON if needed
        if isinstance(data, pd.DataFrame):
            if not data.empty:
                # Reset index to include date as a column
                json_data = data.reset_index().to_json(orient='records', date_format='iso')
                with open(filepath, "w") as f:
                    f.write(json_data)
                logger.info(f"Saved raw data to {filepath}")
            else:
                logger.warning(f"Empty DataFrame not saved: {prefix}")
        elif isinstance(data, dict):
            # Handle dictionary of DataFrames (e.g., related_queries)
            serializable_data = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    serializable_data[key] = {}
                    for subkey, subvalue in value.items():
                        if isinstance(subvalue, pd.DataFrame) and not subvalue.empty:
                            serializable_data[key][subkey] = json.loads(
                                subvalue.reset_index().to_json(orient='records'))
                        else:
                            serializable_data[key][subkey] = None
                elif isinstance(value, pd.DataFrame) and not value.empty:
                    serializable_data[key] = json.loads(value.reset_index().to_json(orient='records'))
                else:
                    serializable_data[key] = None

            with open(filepath, "w") as f:
                json.dump(serializable_data, f, indent=2)
            logger.info(f"Saved raw data to {filepath}")
        else:
            logger.warning(f"Unsupported data type for saving: {type(data)}")


# Example usage
if __name__ == "__main__":
    # Initialize the API client
    api = GoogleTrendsAPI()

    # Example: Get interest over time for Premier League
    interest_data = api.get_interest_over_time(["Denver Nuggets"], timeframe="today 3-m")

    if not interest_data.empty:
        # Print the first few rows
        print(interest_data.head())

        # Get related queries
        related_queries = api.get_related_queries(["Denver Nuggets"])

        if related_queries and "Denver Nuggets" in related_queries:
            top_queries = related_queries["Denver Nuggets"]["top"]
            if isinstance(top_queries, pd.DataFrame) and not top_queries.empty:
                print("\nTop related queries:")
                print(top_queries.head())