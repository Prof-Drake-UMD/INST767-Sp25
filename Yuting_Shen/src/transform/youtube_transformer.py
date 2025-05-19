"""
Transformation module for YouTube data.
Converts raw YouTube API data into structured formats for BigQuery tables.
"""

import json
import pandas as pd
from datetime import datetime
import re
import logging
from dateutil import parser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_sports_related_tags(tags, sports_keywords):
    """
    Extract sports-related tags from a list of video tags.

    Args:
        tags (list): List of video tags
        sports_keywords (list): List of sports-related keywords to match

    Returns:
        list: List of sports-related tags
    """
    if not tags:
        return []

    sports_related = []
    for tag in tags:
        tag_lower = tag.lower()
        for keyword in sports_keywords:
            if keyword.lower() in tag_lower:
                sports_related.append(tag)
                break

    return sports_related


def extract_event_ids_from_title(title, events_data):
    """
    Extract potential event IDs from a video title by matching team names.

    Args:
        title (str): Video title
        events_data (list): List of event data dictionaries

    Returns:
        list: List of potential event IDs
    """
    if not title or not events_data:
        return []

    title_lower = title.lower()
    event_ids = []

    for event in events_data:
        home_team = event.get('home_team_name', '').lower()
        away_team = event.get('away_team_name', '').lower()

        # If both team names are in the title, this is likely a match highlight
        if home_team and away_team and home_team in title_lower and away_team in title_lower:
            event_ids.append(event.get('event_id'))

    return event_ids


def extract_team_ids_from_title(title, teams_data):
    """
    Extract potential team IDs from a video title by matching team names.

    Args:
        title (str): Video title
        teams_data (list): List of team data dictionaries

    Returns:
        list: List of potential team IDs
    """
    if not title or not teams_data:
        return []

    title_lower = title.lower()
    team_ids = []

    for team in teams_data:
        team_name = team.get('team_name', '').lower()
        if team_name and team_name in title_lower:
            team_ids.append(team.get('team_id'))

    return team_ids


def parse_duration(duration_str):
    """
    Parse ISO 8601 duration string (e.g., PT1H30M15S) to seconds.

    Args:
        duration_str (str): ISO 8601 duration string

    Returns:
        int: Duration in seconds
    """
    if not duration_str:
        return None

    try:
        # Remove PT prefix
        duration = duration_str[2:]

        # Initialize seconds
        seconds = 0

        # Parse hours if present
        hour_match = re.search(r'(\d+)H', duration)
        if hour_match:
            seconds += int(hour_match.group(1)) * 3600

        # Parse minutes if present
        minute_match = re.search(r'(\d+)M', duration)
        if minute_match:
            seconds += int(minute_match.group(1)) * 60

        # Parse seconds if present
        second_match = re.search(r'(\d+)S', duration)
        if second_match:
            seconds += int(second_match.group(1))

        return seconds
    except Exception as e:
        logger.warning(f"Could not parse duration {duration_str}: {str(e)}")
        return None


def transform_videos(search_results, video_details, events_data=None, teams_data=None):
    """
    Transform raw YouTube videos data.

    Args:
        search_results (list): Raw data from YouTube API search endpoint
        video_details (dict): Detailed video data from YouTube API videos endpoint
        events_data (list, optional): Transformed events data for linking
        teams_data (list, optional): Transformed teams data for linking

    Returns:
        list: List of dictionaries with transformed video data
    """
    transformed_videos = []
    sports_keywords = ['sport', 'football', 'soccer', 'basketball', 'baseball', 'tennis',
                       'golf', 'rugby', 'cricket', 'hockey', 'league', 'championship',
                       'tournament', 'match', 'game', 'highlight', 'goal', 'score', 'win']

    for video_item in video_details.get('items', []):
        try:
            video_id = video_item.get('id')
            snippet = video_item.get('snippet', {})
            content_details = video_item.get('contentDetails', {})

            # Extract tags and convert to list if present
            tags = snippet.get('tags', [])

            # Extract sports-related tags
            sports_related_tags = extract_sports_related_tags(tags, sports_keywords)

            # Parse published timestamp
            published_at = None
            if snippet.get('publishedAt'):
                try:
                    published_at = parser.parse(snippet.get('publishedAt'))
                except Exception as e:
                    logger.warning(f"Could not parse published date: {snippet.get('publishedAt')}")

            # Parse duration
            duration = parse_duration(content_details.get('duration'))

            # Extract related event and team IDs if available
            related_event_ids = []
            related_team_ids = []

            if events_data:
                related_event_ids = extract_event_ids_from_title(snippet.get('title', ''), events_data)

            if teams_data:
                related_team_ids = extract_team_ids_from_title(snippet.get('title', ''), teams_data)

            # Determine content type based on title and description
            content_type = 'unknown'
            title_lower = snippet.get('title', '').lower()
            description_lower = snippet.get('description', '').lower()

            if any(kw in title_lower or kw in description_lower for kw in ['highlight', 'highlights', 'best moments']):
                content_type = 'highlight'
            elif any(kw in title_lower or kw in description_lower for kw in ['full game', 'full match', 'complete']):
                content_type = 'full_game'
            elif any(kw in title_lower or kw in description_lower for kw in ['interview', 'press', 'conference']):
                content_type = 'interview'
            elif any(kw in title_lower or kw in description_lower for kw in ['analysis', 'breakdown', 'review']):
                content_type = 'analysis'
            elif any(kw in title_lower or kw in description_lower for kw in ['preview', 'upcoming', 'before']):
                content_type = 'preview'

            # Create transformed video object
            transformed_video = {
                'video_id': video_id,
                'channel_id': snippet.get('channelId'),
                'channel_title': snippet.get('channelTitle'),
                'title': snippet.get('title'),
                'description': snippet.get('description'),
                'published_at': published_at.isoformat() if published_at else None,
                'tags': json.dumps(tags) if tags else None,
                'duration': duration,
                'category_id': video_item.get('snippet', {}).get('categoryId'),
                'default_language': snippet.get('defaultLanguage'),
                'content_type': content_type,
                'sports_related_tags': json.dumps(sports_related_tags) if sports_related_tags else None,
                'related_event_ids': json.dumps(related_event_ids) if related_event_ids else None,
                'related_team_ids': json.dumps(related_team_ids) if related_team_ids else None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }

            transformed_videos.append(transformed_video)

        except Exception as e:
            logger.error(f"Error transforming video {video_item.get('id')}: {str(e)}")

    logger.info(f"Transformed {len(transformed_videos)} videos")
    return transformed_videos


def transform_video_metrics(video_metrics_data, previous_metrics=None):
    """
    Transform raw video metrics data.

    Args:
        video_metrics_data (dict): Raw data from YouTube API videos endpoint with statistics
        previous_metrics (dict, optional): Previous day's metrics for calculating daily changes

    Returns:
        list: List of dictionaries with transformed video metrics data
    """
    transformed_metrics = []

    snapshot_date = datetime.now().date().isoformat()

    for video_item in video_metrics_data.get('items', []):
        try:
            video_id = video_item.get('id')
            statistics = video_item.get('statistics', {})

            # Parse metrics
            view_count = int(statistics.get('viewCount', 0))
            like_count = int(statistics.get('likeCount', 0))
            dislike_count = int(statistics.get('dislikeCount', 0)) if 'dislikeCount' in statistics else 0
            favorite_count = int(statistics.get('favoriteCount', 0))
            comment_count = int(statistics.get('commentCount', 0))

            # Calculate engagement rate
            engagement_rate = 0
            if view_count > 0:
                engagement_rate = (like_count + dislike_count + comment_count) / view_count

            # Calculate daily changes if previous metrics are available
            daily_views = 0
            daily_likes = 0
            daily_comments = 0

            if previous_metrics and video_id in previous_metrics:
                prev = previous_metrics[video_id]
                daily_views = view_count - prev.get('view_count', 0)
                daily_likes = like_count - prev.get('like_count', 0)
                daily_comments = comment_count - prev.get('comment_count', 0)

            # Create transformed metrics object
            transformed_metric = {
                'video_id': video_id,
                'snapshot_date': snapshot_date,
                'view_count': view_count,
                'like_count': like_count,
                'dislike_count': dislike_count,
                'favorite_count': favorite_count,
                'comment_count': comment_count,
                'engagement_rate': engagement_rate,
                'daily_views': daily_views,
                'daily_likes': daily_likes,
                'daily_comments': daily_comments,
                'created_at': datetime.now().isoformat()
            }

            transformed_metrics.append(transformed_metric)

        except Exception as e:
            logger.error(f"Error transforming metrics for video {video_item.get('id')}: {str(e)}")

    logger.info(f"Transformed metrics for {len(transformed_metrics)} videos")
    return transformed_metrics


def transform_video_comments(comments_data):
    """
    Transform raw video comments data.

    Args:
        comments_data (list): Raw comments data from YouTube API

    Returns:
        list: List of dictionaries with transformed comments data
    """
    transformed_comments = []

    for comment in comments_data:
        try:
            # Simple sentiment analysis based on keywords
            text = comment.get('text', '')
            sentiment_score = 0

            # Very basic sentiment analysis
            positive_words = ['great', 'good', 'awesome', 'amazing', 'excellent', 'love', 'best', 'fantastic']
            negative_words = ['bad', 'terrible', 'worst', 'hate', 'awful', 'poor', 'disappointing']

            text_lower = text.lower()

            # Count positive and negative word occurrences
            positive_count = sum(1 for word in positive_words if word in text_lower)
            negative_count = sum(1 for word in negative_words if word in text_lower)

            # Calculate simple sentiment score between -1 and 1
            if positive_count > 0 or negative_count > 0:
                sentiment_score = (positive_count - negative_count) / (positive_count + negative_count)

            # Parse published timestamp
            published_at = None
            if comment.get('published_at'):
                try:
                    published_at = parser.parse(comment.get('published_at'))
                except Exception as e:
                    logger.warning(f"Could not parse comment date: {comment.get('published_at')}")

            # Create transformed comment object
            transformed_comment = {
                'comment_id': comment.get('id'),
                'video_id': comment.get('video_id'),
                'author_name': comment.get('author'),
                'text': text,
                'published_at': published_at.isoformat() if published_at else None,
                'like_count': comment.get('like_count', 0),
                'sentiment_score': sentiment_score,
                'parent_id': comment.get('reply_to_id'),
                'created_at': datetime.now().isoformat()
            }

            transformed_comments.append(transformed_comment)

        except Exception as e:
            logger.error(f"Error transforming comment {comment.get('id')}: {str(e)}")

    logger.info(f"Transformed {len(transformed_comments)} comments")
    return transformed_comments