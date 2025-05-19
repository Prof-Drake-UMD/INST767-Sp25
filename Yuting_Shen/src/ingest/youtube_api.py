"""
YouTube API connector for retrieving video and channel data.
Uses the YouTube Data API to retrieve information about videos, channels, and statistics.
"""

import os
import json
import logging
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class YouTubeAPI:
    """
    Client for interacting with the YouTube Data API.
    Retrieves data about videos, channels, and statistics.
    """

    def __init__(self, api_key=None):
        """
        Initialize the YouTubeAPI client.

        Args:
            api_key (str, optional): YouTube Data API key. Defaults to environment variable.
        """
        self.api_key = api_key or os.environ.get('YOUTUBE_API_KEY')

        if not self.api_key:
            raise ValueError("API key is required. Set YOUTUBE_API_KEY environment variable or pass it directly.")

        # Initialize the YouTube API client
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)

    def search_videos(self, query, max_results=10, order="relevance", published_after=None, published_before=None):
        """
        Search for videos by keywords.

        Args:
            query (str): Search query
            max_results (int, optional): Maximum number of results to return
            order (str, optional): Order of results (relevance, date, rating, viewCount)
            published_after (str, optional): RFC 3339 formatted date-time value (1970-01-01T00:00:00Z)
            published_before (str, optional): RFC 3339 formatted date-time value

        Returns:
            list: List of video search result items
        """
        try:
            search_params = {
                'q': query,
                'part': 'id,snippet',
                'maxResults': max_results,
                'order': order,
                'type': 'video'
            }

            # Format the timestamps to RFC 3339 with Z suffix
            if published_after:
                formatted_after = self.format_timestamp(published_after)
                if formatted_after:
                    search_params['publishedAfter'] = formatted_after

            if published_before:
                formatted_before = self.format_timestamp(published_before)
                if formatted_before:
                    search_params['publishedBefore'] = formatted_before

            logger.info(f"Search params: {search_params}")
            search_response = self.youtube.search().list(**search_params).execute()

            # Save raw data to file with timestamp
            search_query = query.replace(" ", "_")[:30]  # Truncate long queries for filename
            self._save_raw_data(search_response, f"search_{search_query}")

            logger.info(f"Retrieved {len(search_response.get('items', []))} videos for query: '{query}'")

            return search_response.get('items', [])

        except HttpError as e:
            logger.error(f"Error searching videos: {str(e)}")
            return []

    def search_sports_videos(self, sport_query, max_results=10, published_after=None, published_before=None):
        """
        Search for sports-related videos within a date range.

        Args:
            sport_query (str): Sport or team to search for
            max_results (int, optional): Maximum number of results to return
            published_after (str, optional): RFC 3339 formatted date (1970-01-01T00:00:00Z)
            published_before (str, optional): RFC 3339 formatted date

        Returns:
            list: List of video search result items
        """
        # Add "sports" or "highlights" to the query for better results
        enhanced_query = f"{sport_query} sports highlights"

        # Call the search_videos method with the enhanced query and date parameters
        return self.search_videos(
            enhanced_query,
            max_results=max_results,
            order="relevance",
            published_after=published_after,
            published_before=published_before
        )



    def get_video_details(self, video_id):
        """
        Get detailed information about a video.

        Args:
            video_id (str): ID of the video

        Returns:
            dict: Video data if found, None otherwise
        """
        try:
            video_response = self.youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            ).execute()

            # Save raw data to file with timestamp
            self._save_raw_data(video_response, f"video_{video_id}")

            if video_response.get('items'):
                logger.info(f"Retrieved details for video {video_id}")
                return video_response['items'][0]
            else:
                logger.warning(f"No video found with ID: {video_id}")
                return None

        except HttpError as e:
            logger.error(f"Error retrieving video details: {str(e)}")
            return None

    def get_channel_details(self, channel_id):
        """
        Get detailed information about a channel.

        Args:
            channel_id (str): ID of the channel

        Returns:
            dict: Channel data if found, None otherwise
        """
        try:
            channel_response = self.youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id
            ).execute()

            # Save raw data to file with timestamp
            self._save_raw_data(channel_response, f"channel_{channel_id}")

            if channel_response.get('items'):
                logger.info(f"Retrieved details for channel {channel_id}")
                return channel_response['items'][0]
            else:
                logger.warning(f"No channel found with ID: {channel_id}")
                return None

        except HttpError as e:
            logger.error(f"Error retrieving channel details: {str(e)}")
            return None

    def get_channel_videos(self, channel_id, max_results=10):
        """
        Get recent videos from a channel.

        Args:
            channel_id (str): ID of the channel
            max_results (int, optional): Maximum number of videos to return

        Returns:
            list: List of video items
        """
        try:
            # First, get the uploads playlist ID for the channel
            channel_response = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id
            ).execute()

            if not channel_response.get('items'):
                logger.warning(f"No channel found with ID: {channel_id}")
                return []

            uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

            # Get the videos from the uploads playlist
            playlist_response = self.youtube.playlistItems().list(
                part='snippet,contentDetails',
                playlistId=uploads_playlist_id,
                maxResults=max_results
            ).execute()

            # Save raw data to file with timestamp
            self._save_raw_data(playlist_response, f"channel_videos_{channel_id}")

            videos = playlist_response.get('items', [])
            logger.info(f"Retrieved {len(videos)} videos for channel {channel_id}")

            return videos

        except HttpError as e:
            logger.error(f"Error retrieving channel videos: {str(e)}")
            return []

    # def search_sports_videos(self, sport_query, max_results=10, published_after=None):
    #     """
    #     Search for sports-related videos.
    #
    #     Args:
    #         sport_query (str): Sport or team to search for
    #         max_results (int, optional): Maximum number of results to return
    #         published_after (str, optional): RFC 3339 formatted date-time value
    #
    #     Returns:
    #         list: List of video search result items
    #     """
    #     # Add "sports" or "highlights" to the query for better results
    #     enhanced_query = f"{sport_query} sports highlights"
    #     return self.search_videos(enhanced_query, max_results, "relevance", published_after)

    def _save_raw_data(self, data, prefix):
        """
        Save raw API response to a file with timestamp.

        Args:
            data (dict): Data to save
            prefix (str): Prefix for the filename
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/raw/youtube_{prefix}_{timestamp}.json"

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Write data to file
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved raw data to {filepath}")

    def get_video_comments(self, video_id, max_results=100):
        """
        Get comments for a specific video.

        Args:
            video_id (str): ID of the video
            max_results (int, optional): Maximum number of comments to return

        Returns:
            list: List of comment items
        """
        try:
            comments = []
            next_page_token = None

            while len(comments) < max_results:
                # Build request
                request = self.youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=min(100, max_results - len(comments)),
                    pageToken=next_page_token
                )

                # Execute request
                response = request.execute()

                # Save raw data
                self._save_raw_data(response, f"comments_{video_id}_{len(comments)}")

                # Extract comments
                for item in response['items']:
                    comment = item['snippet']['topLevelComment']['snippet']
                    comments.append({
                        'id': item['id'],
                        'video_id': video_id,
                        'author': comment['authorDisplayName'],
                        'text': comment['textDisplay'],
                        'like_count': comment['likeCount'],
                        'published_at': comment['publishedAt'],
                        'updated_at': comment['updatedAt'],
                    })

                # Check if there are more comments
                next_page_token = response.get('nextPageToken')
                if not next_page_token or len(comments) >= max_results:
                    break

            logger.info(f"Retrieved {len(comments)} comments for video {video_id}")
            return comments

        except HttpError as e:
            # Check if the error is because comments are disabled
            if 'disabled comments' in str(e).lower():
                logger.info(f"Comments are disabled for video {video_id}")
                return []
            logger.error(f"Error retrieving comments: {str(e)}")
            return []

    # Add this helper method to your youtube_api.py class

    def format_timestamp(self, date_str):
        """
        Format a date string to RFC 3339 format required by YouTube API.

        Args:
            date_str (str): Date string in YYYY-MM-DD format

        Returns:
            str: RFC 3339 formatted timestamp with Z suffix (UTC)
        """
        if not date_str:
            return None

        # Make sure we're dealing with a string
        if not isinstance(date_str, str):
            logger.warning(f"Expected string for date_str, got {type(date_str)}")
            if hasattr(date_str, 'isoformat'):  # If it's a datetime object
                return date_str.isoformat() + 'Z'
            return None

        try:
            # If it's already in RFC 3339 format with Z or timezone, return it
            if date_str.endswith('Z') or '+' in date_str:
                return date_str

            # If it's just a date (YYYY-MM-DD), append time and Z
            if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
                return f"{date_str}T00:00:00Z"

            # If it has time but no Z or timezone, add Z
            if 'T' in date_str and not (date_str.endswith('Z') or '+' in date_str):
                return f"{date_str}Z"

            # Otherwise, try to parse and format it
            from datetime import datetime
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            self.logger.error(f"Error formatting timestamp {date_str}: {str(e)}")
            return None


# Example usage
if __name__ == "__main__":
    # Initialize the API client
    api = YouTubeAPI("AIzaSyDHMMEqzjYdqGkca8duq5YRGm_lGBzN074")

    # Example: Search for highlights
    videos = api.search_sports_videos("Kansas City Chiefs", max_results=3)

    # Print information about the first video
    if videos:
        video = videos[0]
        video_id = video['id']['videoId']
        print(f"Video: {video['snippet']['title']}")

        # Get more details about the video
        details = api.get_video_details(video_id)
        if details:
            print(f"View count: {details['statistics'].get('viewCount', 'unknown')}")
            print(f"Like count: {details['statistics'].get('likeCount', 'unknown')}")