"""
Pipeline for processing Google Trends data.
"""

import os
import sys
import json
from datetime import datetime, timedelta
import logging

from src.pipeline.sports_pipeline import SportsPipeline
from src.pipeline.youtube_pipeline import YouTubePipeline

# Add the src directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import API modules
from src.ingest.trends_api import GoogleTrendsAPI
from src.transform.trends_transformer import transform_search_trends, transform_regional_interest, \
    transform_related_queries
from src.transform.data_validator import validate_search_trends
from src.utils.data_writer import write_to_csv, write_errors_to_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrendsPipeline:

    """Pipeline for processing Google Trends data."""
    def __init__(self, cache_dir="data/trends_cache", cache_max_age_days=7,
                 request_interval=60, retries=3, backoff_factor=2,
                 session_cooldown=60, hl='en-US', tz=360):
        """
        Initialize the pipeline.

        Args:
            cache_dir (str): Directory to store cached data
            cache_max_age_days (int): Maximum age of cached data in days
            request_interval (int): Minimum seconds between requests
            retries (int): Number of retry attempts for failed requests
            backoff_factor (int): Backoff factor for retries
            session_cooldown (int): Time to wait before creating a new session
            hl (str): Language for Google Trends
            tz (int): Timezone offset
        """
        self.api = GoogleTrendsAPI(
            hl=hl,
            tz=tz,
            cache_dir=cache_dir,
            cache_max_age_days=cache_max_age_days,
            request_interval=request_interval,
            retries=retries,
            backoff_factor=backoff_factor,
            session_cooldown=session_cooldown
        )
        self.search_trends_data = []
        self.regional_interest_data = []
        self.related_queries_data = []

    def fetch_interest_over_time(self, keywords, timeframe='today 3-m'):
        """
        Fetch interest over time data from the API.

        Args:
            keywords (list): List of search terms
            timeframe (str): Time period for data

        Returns:
            pandas.DataFrame: Interest over time data
        """
        logger.info(f"Fetching interest over time data for keywords: {keywords}")
        return self.api.get_interest_over_time(keywords, timeframe)

    def fetch_interest_by_region(self, keywords, resolution='REGION', timeframe='today 3-m'):
        """
        Fetch interest by region data from the API.

        Args:
            keywords (list): List of search terms
            resolution (str): Geographic level
            timeframe (str): Time period for data

        Returns:
            pandas.DataFrame: Interest by region data
        """
        logger.info(f"Fetching interest by region data for keywords: {keywords}")
        return self.api.get_interest_by_region(keywords, resolution, timeframe)

    def fetch_related_queries(self, keywords, timeframe='today 3-m'):
        """
        Fetch related queries data from the API.

        Args:
            keywords (list): List of search terms
            timeframe (str): Time period for data

        Returns:
            dict: Related queries data
        """
        logger.info(f"Fetching related queries data for keywords: {keywords}")
        return self.api.get_related_queries(keywords, timeframe)

    def fetch_event_interest(self, event, teams_data=None):
        """
        Fetch interest data for a specific event.

        Args:
            event (dict): Event data
            teams_data (list, optional): Teams data for additional keywords

        Returns:
            dict: Event interest data
        """
        # Add more validation and logging
        if not event:
            logger.warning("Event data is None or empty")
            return {}

        event_id = event.get('event_id')
        if not event_id:
            logger.warning("Event ID is missing")
            return {}

        event_date = event.get('event_date')
        if not event_date:
            logger.warning(f"Event date is missing for event {event_id}")

        home_team_id = event.get('home_team_id')
        away_team_id = event.get('away_team_id')
        event_name = event.get('event_name', '')

        # Get team names from teams data if available
        keywords = []
        team_ids = []

        if teams_data:
            # Find team names
            for team in teams_data:
                if team.get('team_id') == home_team_id or team.get('team_id') == away_team_id:
                    team_name = team.get('team_name')
                    if team_name:
                        keywords.append(team_name)
                        team_ids.append(team.get('team_id'))

        # If no team names found, use event name if available
        if not keywords and event_name:
            # [UPDATED] Check if event_name is a string before using string methods
            if isinstance(event_name, str):
                # Clean event name to make it more search-friendly
                clean_name = event_name.replace(" vs ", " ").replace(" at ", " ")
                keywords.append(clean_name)
                logger.info(f"Using event name as keyword: {clean_name}")
            else:
                logger.warning(f"Event name is not a string: {type(event_name)}")

        # Default to empty list if no keywords found
        if not keywords:
            logger.warning(f"No keywords found for event {event_id}")
            return {}

        # Create timeframe centered on event date (2 weeks before and after)
        # Use first keyword only to improve reliability
        primary_keyword = keywords[0]
        logger.info(f"Fetching interest data for event {event_id} with keyword: {primary_keyword}")

        # Try with a simpler approach to ensure success
        try:
            # [UPDATED] Ensure we're using a string timeframe
            return self.api.get_interest_for_team_event(primary_keyword, event_date, 'today 1-m')
        except Exception as e:
            logger.error(f"Error fetching event interest: {str(e)}")
            return {}

    def process_search_trends(self, interest_over_time_data, related_event_id=None, related_team_id=None):
        """
        Process search trends data through the pipeline.

        Args:
            interest_over_time_data (DataFrame): Raw interest over time data
            related_event_id (str, optional): ID of related event
            related_team_id (str, optional): ID of related team

        Returns:
            tuple: (transformed_trends, csv_path)
        """
        # Transform search trends data
        transformed_trends = transform_search_trends(interest_over_time_data, related_event_id, related_team_id)

        # Validate transformed data
        valid_trends, errors = validate_search_trends(transformed_trends)

        # Write validation errors if any
        if errors:
            write_errors_to_json(errors, 'search_trends_validation_errors')

        # Write valid data to CSV
        csv_path = write_to_csv(valid_trends, 'search_trends')

        # Store search trends data for later use
        self.search_trends_data.extend(valid_trends)

        return valid_trends, csv_path

    def process_regional_interest(self, interest_by_region_data, date, related_event_id=None, related_team_id=None):
        """
        Process regional interest data through the pipeline.

        Args:
            interest_by_region_data (DataFrame): Raw interest by region data
            date (str): Date for the regional interest data
            related_event_id (str, optional): ID of related event
            related_team_id (str, optional): ID of related team

        Returns:
            tuple: (transformed_regional_interest, csv_path)
        """
        # Transform regional interest data
        transformed_regional = transform_regional_interest(interest_by_region_data, date, related_event_id,
                                                           related_team_id)

        # Write valid data to CSV
        csv_path = write_to_csv(transformed_regional, 'regional_interest')

        # Store regional interest data for later use
        self.regional_interest_data.extend(transformed_regional)

        return transformed_regional, csv_path

    def process_related_queries(self, related_queries_data, date):
        """
        Process related queries data through the pipeline.

        Args:
            related_queries_data (dict): Raw related queries data
            date (str): Date for the related queries data

        Returns:
            tuple: (transformed_queries, csv_path)
        """
        # Transform related queries data
        transformed_queries = transform_related_queries(related_queries_data, date)

        # Write valid data to CSV
        csv_path = write_to_csv(transformed_queries, 'related_queries')

        # Store related queries data for later use
        self.related_queries_data.extend(transformed_queries)

        return transformed_queries, csv_path

    def run(self, keywords, timeframe='today 1-m', events_data=None, teams_data=None):
        """
        Run the complete Google Trends data pipeline with improved robustness.
        """
        results = {
            'start_time': datetime.now().isoformat(),
            'keywords': keywords,
            'timeframe': timeframe,
            'status': 'success',
            'errors': [],
            'csv_paths': {}
        }

        # Input validation
        if not keywords:
            logger.warning("No keywords provided to trends pipeline")
            results['status'] = 'error'
            results['errors'].append("No keywords provided")
            return results

        # Normalize keywords
        if isinstance(keywords, str):
            keywords = [keywords]

        # [UPDATED] Make sure timeframe is a string
        if not isinstance(timeframe, str):
            logger.warning(f"timeframe must be a string, got {type(timeframe)}. Using default.")
            timeframe = 'today 1-m'

        # Validate keywords
        valid_keywords = [k for k in keywords if k and isinstance(k, str) and k.strip()]
        if not valid_keywords:
            logger.warning("No valid keywords after filtering")
            results['status'] = 'error'
            results['errors'].append("No valid keywords after filtering")
            return results

        # Use only first keyword for more reliable results
        primary_keyword = valid_keywords[0]
        logger.info(f"Using primary keyword for trends: {primary_keyword}")

        # Use a simpler timeframe to improve reliability
        safe_timeframe = 'today 1-m'

        try:
            # Fetch general interest over time with a safe approach
            interest_over_time = self.fetch_interest_over_time([primary_keyword], safe_timeframe)

            # Process search trends data
            if not interest_over_time.empty:
                transformed_trends, trends_csv_path = self.process_search_trends(interest_over_time)
                results['csv_paths']['search_trends'] = trends_csv_path
                results['trends_count'] = len(transformed_trends)
            else:
                logger.warning("Interest over time data is empty")
                results['trends_count'] = 0

            # Get current date for regional interest (can't specify historical regions)
            date = datetime.now().date().isoformat()

            # Try fetching and processing interest by region
            try:
                interest_by_region = self.fetch_interest_by_region([primary_keyword])
                if not interest_by_region.empty:
                    transformed_regional, regional_csv_path = self.process_regional_interest(interest_by_region, date)
                    results['csv_paths']['regional_interest'] = regional_csv_path
                    results['regional_count'] = len(transformed_regional)
                else:
                    logger.warning("Interest by region data is empty")
                    results['regional_count'] = 0
            except Exception as e:
                logger.error(f"Error processing regional interest: {str(e)}")
                results['errors'].append(f"Regional interest error: {str(e)}")
                results['regional_count'] = 0

            # Try fetching and processing related queries
            try:
                related_queries = self.fetch_related_queries([primary_keyword], safe_timeframe)
                if related_queries:
                    transformed_queries, queries_csv_path = self.process_related_queries(related_queries, date)
                    results['csv_paths']['related_queries'] = queries_csv_path
                    results['queries_count'] = len(transformed_queries)
                else:
                    logger.warning("Related queries data is empty")
                    results['queries_count'] = 0
            except Exception as e:
                logger.error(f"Error processing related queries: {str(e)}")
                results['errors'].append(f"Related queries error: {str(e)}")
                results['queries_count'] = 0

            # Process event-specific interest if events data is provided (limit to just 1-2 events for reliability)
            if events_data and len(events_data) > 0:
                event_interest_results = []
                # Only try the first 2 events to reduce API load
                for event in events_data[:2]:
                    try:
                        event_interest = self.fetch_event_interest(event, teams_data)

                        if event_interest and 'interest_over_time' in event_interest and not event_interest[
                            'interest_over_time'].empty:
                            # Process event-specific search trends
                            event_trends, event_trends_csv_path = self.process_search_trends(
                                event_interest['interest_over_time'],
                                related_event_id=event.get('event_id'),
                                related_team_id=event.get('home_team_id')
                            )

                            event_interest_results.append({
                                'event_id': event.get('event_id'),
                                'event_name': event.get('event_name'),
                                'trends_count': len(event_trends)
                            })
                    except Exception as e:
                        error_msg = f"Error processing event interest for {event.get('event_id', 'unknown')}: {str(e)}"
                        logger.error(error_msg)
                        results['errors'].append(error_msg)

                results['event_interest_results'] = event_interest_results

            # Set status based on overall results
            if results['trends_count'] > 0 or results['regional_count'] > 0 or results['queries_count'] > 0:
                if results['errors']:
                    results['status'] = 'partial_success'
                else:
                    results['status'] = 'success'
            else:
                results['status'] = 'error'
                if not results['errors']:
                    results['errors'].append("No trends data could be retrieved")

        except Exception as e:
            logger.error(f"Error in Trends pipeline: {str(e)}")
            results['status'] = 'error'
            results['errors'].append(str(e))

        results['end_time'] = datetime.now().isoformat()
        return results