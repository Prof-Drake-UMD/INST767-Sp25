"""
Pipeline for processing YouTube data.
"""

import os
import sys
import json
from datetime import datetime
import logging

# Add the src directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import API modules
from src.ingest.youtube_api import YouTubeAPI
from src.transform.youtube_transformer import transform_videos, transform_video_metrics, transform_video_comments
from src.transform.data_validator import validate_videos, validate_video_metrics
from src.utils.data_writer import write_to_csv, write_errors_to_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubePipeline:
    """Pipeline for processing YouTube data."""

    def __init__(self, api_key=None):
        """
        Initialize the pipeline.

        Args:
            api_key (str, optional): YouTube API key
        """
        self.api = YouTubeAPI(api_key)
        self.videos_data = []
        self.video_metrics_data = []
        self.video_comments_data = []

    def fetch_sports_videos(self, query, max_results=10, published_after=None, published_before=None):
        """
        Fetch sports videos from the API with date range support.

        Args:
            query (str): Search query
            max_results (int): Maximum number of results to fetch
            published_after (str, optional): RFC 3339 formatted date
            published_before (str, optional): RFC 3339 formatted date

        Returns:
            list: List of video search results
        """
        logger.info(f"Searching for videos with query '{query}' from {published_after} to {published_before}")
        return self.api.search_sports_videos(query, max_results, published_after, published_before)


    def fetch_video_details(self, video_ids):
        """
        Fetch video details from the API.

        Args:
            video_ids (list): List of video IDs to fetch details for

        Returns:
            dict: Raw video details data
        """
        video_details = {'items': []}

        for video_id in video_ids:
            logger.info(f"Fetching details for video {video_id}")
            details = self.api.get_video_details(video_id)
            if details:
                video_details['items'].append(details)

        return video_details

    def fetch_video_comments(self, video_ids, max_results=100):
        """
        Fetch video comments from the API.

        Args:
            video_ids (list): List of video IDs to fetch comments for
            max_results (int): Maximum number of comments to fetch per video

        Returns:
            list: List of comments
        """
        all_comments = []

        for video_id in video_ids:
            logger.info(f"Fetching comments for video {video_id}")
            comments = self.api.get_video_comments(video_id, max_results)
            all_comments.extend(comments)

        return all_comments

    def process_videos(self, search_results, video_details, events_data=None, teams_data=None):
        """
        Process videos data through the pipeline.

        Args:
            search_results (list): Raw video search results
            video_details (dict): Raw video details
            events_data (list, optional): Transformed events data for linking
            teams_data (list, optional): Transformed teams data for linking

        Returns:
            tuple: (transformed_videos, csv_path)
        """
        # Transform videos data
        transformed_videos = transform_videos(search_results, video_details, events_data, teams_data)

        # Validate transformed data
        valid_videos, errors = validate_videos(transformed_videos)

        # Write validation errors if any
        if errors:
            write_errors_to_json(errors, 'videos_validation_errors')

        # Write valid data to CSV
        csv_path = write_to_csv(valid_videos, 'youtube_videos')

        # Store videos data for later use
        self.videos_data = valid_videos

        return valid_videos, csv_path

    def process_video_metrics(self, video_details, previous_metrics=None):
        """
        Process video metrics data through the pipeline.

        Args:
            video_details (dict): Raw video details with statistics
            previous_metrics (dict, optional): Previous metrics for calculating daily changes

        Returns:
            tuple: (transformed_metrics, csv_path)
        """
        # Transform video metrics data
        transformed_metrics = transform_video_metrics(video_details, previous_metrics)

        # Validate transformed data
        valid_metrics, errors = validate_video_metrics(transformed_metrics)

        # Write validation errors if any
        if errors:
            write_errors_to_json(errors, 'video_metrics_validation_errors')

        # Write valid data to CSV
        csv_path = write_to_csv(valid_metrics, 'video_metrics')

        # Store video metrics data for later use
        self.video_metrics_data = valid_metrics

        return valid_metrics, csv_path

    def process_comments(self, comments_data):
        """
        Process video comments data through the pipeline.

        Args:
            comments_data (list): Raw comments data

        Returns:
            tuple: (transformed_comments, csv_path)
        """
        # Transform comments data
        transformed_comments = transform_video_comments(comments_data)

        # Write data to CSV
        csv_path = write_to_csv(transformed_comments, 'video_comments')

        # Store comments data for later use
        self.video_comments_data = transformed_comments

        return transformed_comments, csv_path




    def run(self, query, max_results=10, published_after=None, published_before=None, events_data=None,
            teams_data=None):
        """
        Run the complete YouTube data pipeline.

        Args:
            query (str): Search query for videos
            max_results (int): Maximum number of videos to fetch
            published_after (str, optional): Date for filtering videos after (YYYY-MM-DD)
            published_before (str, optional): Date for filtering videos before (YYYY-MM-DD)
            events_data (list, optional): Transformed events data for linking
            teams_data (list, optional): Transformed teams data for linking

        Returns:
            dict: Dictionary with results of the pipeline
        """
        results = {
            'start_time': datetime.now().isoformat(),
            'query': query,
            'status': 'success',
            'errors': [],
            'csv_paths': {}
        }

        try:
            # Fetch videos data with date range
            #search_results = self.fetch_sports_videos(query, max_results, published_after, published_before)

            # Format dates if needed to ensure they have proper RFC 3339 format
            # Check if published_after is a string before calling endswith
            if published_after and isinstance(published_after, str) and not published_after.endswith('Z'):
                if len(published_after) == 10:  # Just YYYY-MM-DD
                    published_after = f"{published_after}T00:00:00Z"

            # Check if published_before is a string before calling endswith
            if published_before and isinstance(published_before, str) and not published_before.endswith('Z'):
                if len(published_before) == 10:  # Just YYYY-MM-DD
                    published_before = f"{published_before}T00:00:00Z"

            # Fetch videos data with date range
            logger.info(
                f"Searching for videos with query: {query}, after: {published_after}, before: {published_before}")
            search_results = self.fetch_sports_videos(
                query,
                max_results=max_results,
                published_after=published_after,
                published_before=published_before
            )

            # Extract video IDs from search results
            video_ids = [video['id']['videoId'] for video in search_results]

            # Fetch video details

            video_details = self.fetch_video_details(video_ids)

            # Process videos data
            transformed_videos, videos_csv_path = self.process_videos(search_results, video_details, events_data,
                                                                      teams_data)
            results['csv_paths']['youtube_videos'] = videos_csv_path
            results['videos_count'] = len(transformed_videos)

            # Process video metrics
            transformed_metrics, metrics_csv_path = self.process_video_metrics(video_details)
            results['csv_paths']['video_metrics'] = metrics_csv_path
            results['metrics_count'] = len(transformed_metrics)

            # Fetch and process comments
            comments_data = self.fetch_video_comments(video_ids)
            transformed_comments, comments_csv_path = self.process_comments(comments_data)
            results['csv_paths']['video_comments'] = comments_csv_path
            results['comments_count'] = len(transformed_comments)

        except Exception as e:
            logger.error(f"Error in YouTube pipeline: {str(e)}")
            results['status'] = 'error'
            results['errors'].append(str(e))

        results['end_time'] = datetime.now().isoformat()
        return results