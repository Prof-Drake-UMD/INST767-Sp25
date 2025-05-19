"""
Integration pipeline that combines data from all sources.
"""

import os
import sys
import json
from datetime import datetime, timedelta
import logging

# Add the src directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# Import modules with direct src-prefixed imports
from src.pipeline.sports_pipeline import SportsPipeline
from src.pipeline.youtube_pipeline import YouTubePipeline
from src.pipeline.trends_pipeline import TrendsPipeline
from src.transform.integration_transformer import create_integrated_events_analysis
from src.transform.data_validator import validate_integrated_analysis
from src.utils.data_writer import write_to_csv, write_errors_to_json


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegrationPipeline:
    """Pipeline for integrating data from all sources."""

    def __init__(self, youtube_api_key=None, sportsdb_api_key=None, trends_config=None):
        """
        Initialize the integration pipeline.

        Args:
            youtube_api_key (str, optional): YouTube API key
            sportsdb_api_key (str, optional): TheSportsDB API key
            trends_config (dict, optional): Configuration for Google Trends API
        """
        self.sports_pipeline = SportsPipeline(sportsdb_api_key)
        self.youtube_pipeline = YouTubePipeline(youtube_api_key)


        # Configure Trends API with caching parameters
        if trends_config is None:
            trends_config = {
                'cache_dir': "data/trends_cache",
                'cache_max_age_days': 7,
                'request_interval': 60,
                'retries': 3,
                'backoff_factor': 2,
                'session_cooldown': 60
            }
        self.trends_pipeline = TrendsPipeline(**trends_config)

        self.events_data = []
        self.teams_data = []
        self.videos_data = []
        self.video_metrics_data = []
        self.search_trends_data = []
        self.integrated_analysis_data = []

    def run_sports_pipeline(self, league_id, team_search=None):
        """
        Run the sports data pipeline.

        Args:
            league_id (str): ID of the league to process
            team_search (str, optional): Team name to search for

        Returns:
            dict: Results of the sports pipeline
        """
        logger.info(f"Running sports pipeline for league {league_id}")
        results = self.sports_pipeline.run(league_id, team_search)

        # Store data for later use
        self.events_data = self.sports_pipeline.events_data
        self.teams_data = self.sports_pipeline.teams_data

        return results

    def run_youtube_pipeline(self, query, max_results=10):
        """
        Run the YouTube data pipeline.

        Args:
            query (str): Search query for videos
            max_results (int): Maximum number of videos to fetch

        Returns:
            dict: Results of the YouTube pipeline
        """
        logger.info(f"Running YouTube pipeline for query '{query}'")
        results = self.youtube_pipeline.run(query, max_results, self.events_data, self.teams_data)

        # Store data for later use
        self.videos_data = self.youtube_pipeline.videos_data
        self.video_metrics_data = self.youtube_pipeline.video_metrics_data

        return results

    def run_trends_pipeline(self, keywords):
        """
        Run the Google Trends data pipeline with improved error handling.
        """
        logger.info(f"Running Trends pipeline for keywords {keywords}")

        # [UPDATED] Validate and normalize input
        if not keywords:
            logger.warning("No keywords provided to trends pipeline")
            return {'status': 'error', 'error': 'No keywords provided', 'csv_paths': {}}

        # [UPDATED] If a string is passed, convert to list
        if isinstance(keywords, str):
            keywords = [keywords]

        # [UPDATED] Filter out invalid keywords
        valid_keywords = [k for k in keywords if k and isinstance(k, str) and k.strip()]
        if not valid_keywords:
            logger.warning("No valid keywords after filtering")
            return {'status': 'error', 'error': 'No valid keywords', 'csv_paths': {}}

        # [UPDATED] Use simpler timeframe for reliability
        timeframe = 'today 1-m'

        # Now run the pipeline with validated keywords
        try:
            results = self.trends_pipeline.run(valid_keywords, timeframe, self.events_data, self.teams_data)

            # Store data even if partial success
            if results.get('status') in ('success', 'partial_success'):
                self.search_trends_data = self.trends_pipeline.search_trends_data

            return results
        except Exception as e:
            logger.error(f"Error in trends pipeline: {str(e)}")
            return {
                'status': 'error',
                'errors': [str(e)],
                'csv_paths': {}
            }


    # def handle_pipeline_error(self, error, source):
    #     """
    #     Handle a pipeline error.
    #
    #     Args:
    #         error (Exception): Error to handle
    #         source (str): Source of the error
    #
    #     Returns:
    #         dict: Error details
    #     """
    #     from Yuting_Shen.src.utils.error_handler import handle_error, PipelineError
    #
    #     # If it's already a PipelineError, use it directly
    #     if isinstance(error, PipelineError):
    #         return handle_error(error)
    #
    #     # Otherwise, create a PipelineError with the source
    #     pipeline_error = PipelineError(
    #         message=str(error),
    #         source=source,
    #         details={
    #             'error_type': type(error).__name__,
    #             'pipeline': self.__class__.__name__
    #         }
    #     )
    #
    #     return handle_error(pipeline_error)

    def handle_pipeline_error(self, error, source):
        """
        Handle a pipeline error.

        Args:
            error (Exception): Error to handle
            source (str): Source of the error

        Returns:
            dict: Error details
        """
        # Create a simple error details dictionary
        error_details = {
            'error': str(error),
            'error_type': type(error).__name__,
            'source': source,
            'timestamp': datetime.now().isoformat()
        }

        logger.error(f"Error in {source}: {str(error)}")
        return error_details

    def create_integrated_analysis(self):
        """
        Create integrated analysis from all data sources.

        Returns:
            tuple: (integrated_analysis, csv_path)
        """
        logger.info("Creating integrated event analysis")

        # Create integrated analysis
        integrated_analysis = create_integrated_events_analysis(
            self.events_data,
            self.videos_data,
            self.video_metrics_data,
            self.search_trends_data
        )

        # Validate integrated analysis
        valid_analysis, errors = validate_integrated_analysis(integrated_analysis)

        # Write validation errors if any
        if errors:
            write_errors_to_json(errors, 'integrated_analysis_validation_errors')

        # Write valid data to CSV
        csv_path = write_to_csv(valid_analysis, 'integrated_events_analysis')

        # Store integrated analysis for later use
        self.integrated_analysis_data = valid_analysis


        return valid_analysis, csv_path

    def run(self, league_id, team_search=None, youtube_query=None, trend_keywords=None, max_results=10):
        """
        Run the complete integration pipeline.

        Args:
            league_id (str): ID of the league to process
            team_search (str, optional): Team name to search for
            youtube_query (str, optional): Search query for YouTube videos
            trend_keywords (list, optional): List of keywords for Google Trends
            max_results (int, optional): Maximum number of YouTube videos to fetch

        Returns:
            dict: Dictionary with results of the integration pipeline
        """
        results = {
            'start_time': datetime.now().isoformat(),
            'league_id': league_id,
            'status': 'success',
            'errors': [],
            'csv_paths': {}
        }

        try:
            # Run sports pipeline
            try:
                sports_results = self.run_sports_pipeline(league_id, team_search)
                results['sports_results'] = sports_results
                results['csv_paths'].update(sports_results.get('csv_paths', {}))
            except Exception as e:
                error_details = self.handle_pipeline_error(e, 'sports_pipeline')
                results['errors'].append(error_details)
                logger.error(f"Sports pipeline failed: {str(e)}")

            # Generate YouTube query if not provided
            if not youtube_query and self.teams_data:
                # Use team names as query
                team_names = [team.get('team_name') for team in self.teams_data[:2]]
                youtube_query = f"{' vs '.join(team_names)} highlights"

            # Run YouTube pipeline if query is available
            if youtube_query:
                try:
                    # Videos from the last 30 days
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=30)

                    # Format in RFC 3339 format with Z
                    published_after = start_date.strftime('%Y-%m-%dT00:00:00Z')
                    published_before = end_date.strftime('%Y-%m-%dT23:59:59Z')

                    youtube_results = self.youtube_pipeline.run(
                        youtube_query,
                        max_results=max_results,
                        published_after=published_after,
                        published_before=published_before,
                        events_data=self.events_data,
                        teams_data=self.teams_data
                    )
                    results['youtube_results'] = youtube_results
                    results['csv_paths'].update(youtube_results.get('csv_paths', {}))
                except Exception as e:
                    error_details = self.handle_pipeline_error(e, 'youtube_pipeline')
                    results['errors'].append(error_details)
                    logger.error(f"YouTube pipeline failed: {str(e)}")

            # Generate trend keywords if not provided
            if not trend_keywords and self.teams_data:
                # Use team names as keywords
                trend_keywords = [team.get('team_name') for team in self.teams_data[:5]]

            # Run Trends pipeline if keywords are available
            if trend_keywords:
                try:
                    trends_results = self.run_trends_pipeline(trend_keywords)
                    results['trends_results'] = trends_results
                    results['csv_paths'].update(trends_results.get('csv_paths', {}))
                except Exception as e:
                    error_details = self.handle_pipeline_error(e, 'trends_pipeline')
                    results['errors'].append(error_details)
                    logger.error(f"Trends pipeline failed: {str(e)}")

            # Create integrated analysis if we have data from all pipelines

            if self.events_data and (self.videos_data or self.search_trends_data):
                try:
                    integrated_analysis, integrated_csv_path = self.create_integrated_analysis()
                    results['csv_paths']['integrated_events_analysis'] = integrated_csv_path
                    results['integrated_analysis_count'] = len(integrated_analysis)
                except Exception as e:
                    error_details = self.handle_pipeline_error(e, 'integrated_analysis')
                    results['errors'].append(error_details)
                    logger.error(f"Integrated analysis failed: {str(e)}")

            # If there were any errors, update the status
            if results['errors']:
                results['status'] = 'partial_success' if self.events_data else 'error'

        except Exception as e:
            error_details = self.handle_pipeline_error(e, 'integration_pipeline')
            results['errors'].append(error_details)
            results['status'] = 'error'
            logger.error(f"Integration pipeline failed: {str(e)}")

        results['end_time'] = datetime.now().isoformat()
        return results

