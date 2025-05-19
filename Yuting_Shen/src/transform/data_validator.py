"""
Data validation module for ensuring data quality.
Validates transformed data before output to CSV.
"""

import json
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_events(events_data):
    """
    Validate events data.

    Args:
        events_data (list): List of transformed events

    Returns:
        tuple: (valid_events, validation_errors)
    """
    valid_events = []
    validation_errors = []

    for event in events_data:
        # Required fields
        required_fields = ['event_id', 'event_name', 'event_date']
        missing_fields = [field for field in required_fields if not event.get(field)]

        if missing_fields:
            error = {
                'record': event,
                'error': f"Missing required fields: {', '.join(missing_fields)}"
            }
            validation_errors.append(error)
            continue

        # Validate date format
        if event.get('event_date'):
            try:
                datetime.strptime(event.get('event_date'), '%Y-%m-%d')
            except ValueError:
                error = {
                    'record': event,
                    'error': f"Invalid date format: {event.get('event_date')}"
                }
                validation_errors.append(error)
                continue

        # Validate scores if present
        if event.get('home_score') is not None and not isinstance(event.get('home_score'), (int, float)):
            error = {
                'record': event,
                'error': f"Invalid home score: {event.get('home_score')}"
            }
            validation_errors.append(error)
            continue

        if event.get('away_score') is not None and not isinstance(event.get('away_score'), (int, float)):
            error = {
                'record': event,
                'error': f"Invalid away score: {event.get('away_score')}"
            }
            validation_errors.append(error)
            continue

        # Add event to valid list if it passes all validations
        valid_events.append(event)

    logger.info(f"Validated {len(valid_events)} events, found {len(validation_errors)} errors")
    return valid_events, validation_errors


def validate_teams(teams_data):
    """
    Validate teams data.

    Args:
        teams_data (list): List of transformed teams

    Returns:
        tuple: (valid_teams, validation_errors)
    """
    valid_teams = []
    validation_errors = []

    for team in teams_data:
        # Required fields
        required_fields = ['team_id', 'team_name']
        missing_fields = [field for field in required_fields if not team.get(field)]

        if missing_fields:
            error = {
                'record': team,
                'error': f"Missing required fields: {', '.join(missing_fields)}"
            }
            validation_errors.append(error)
            continue

        # Validate founded year if present
        if team.get('founded') is not None and not isinstance(team.get('founded'), (int, float)):
            error = {
                'record': team,
                'error': f"Invalid founded year: {team.get('founded')}"
            }
            validation_errors.append(error)
            continue

        # Add team to valid list if it passes all validations
        valid_teams.append(team)

    logger.info(f"Validated {len(valid_teams)} teams, found {len(validation_errors)} errors")
    return valid_teams, validation_errors


def validate_videos(videos_data):
    """
    Validate videos data.

    Args:
        videos_data (list): List of transformed videos

    Returns:
        tuple: (valid_videos, validation_errors)
    """
    valid_videos = []
    validation_errors = []

    for video in videos_data:
        # Required fields
        required_fields = ['video_id', 'title']
        missing_fields = [field for field in required_fields if not video.get(field)]

        if missing_fields:
            error = {
                'record': video,
                'error': f"Missing required fields: {', '.join(missing_fields)}"
            }
            validation_errors.append(error)
            continue

        # Validate arrays stored as JSON
        json_fields = ['tags', 'sports_related_tags', 'related_event_ids', 'related_team_ids']

        for field in json_fields:
            if video.get(field):
                try:
                    json.loads(video.get(field))
                except json.JSONDecodeError:
                    error = {
                        'record': video,
                        'error': f"Invalid JSON in field {field}: {video.get(field)}"
                    }
                    validation_errors.append(error)
                    continue

        # Add video to valid list if it passes all validations
        valid_videos.append(video)

    logger.info(f"Validated {len(valid_videos)} videos, found {len(validation_errors)} errors")
    return valid_videos, validation_errors


def validate_video_metrics(metrics_data):
    """
    Validate video metrics data.

    Args:
        metrics_data (list): List of transformed video metrics

    Returns:
        tuple: (valid_metrics, validation_errors)
    """
    valid_metrics = []
    validation_errors = []

    for metric in metrics_data:
        # Required fields
        required_fields = ['video_id', 'snapshot_date']
        missing_fields = [field for field in required_fields if not metric.get(field)]

        if missing_fields:
            error = {
                'record': metric,
                'error': f"Missing required fields: {', '.join(missing_fields)}"
            }
            validation_errors.append(error)
            continue

        # Validate date format
        if metric.get('snapshot_date'):
            try:
                datetime.strptime(metric.get('snapshot_date'), '%Y-%m-%d')
            except ValueError:
                error = {
                    'record': metric,
                    'error': f"Invalid date format: {metric.get('snapshot_date')}"
                }
                validation_errors.append(error)
                continue

        # Validate numeric fields
        numeric_fields = ['view_count', 'like_count', 'dislike_count', 'favorite_count',
                          'comment_count', 'daily_views', 'daily_likes', 'daily_comments']

        for field in numeric_fields:
            if metric.get(field) is not None and not isinstance(metric.get(field), (int, float)):
                try:
                    metric[field] = int(metric.get(field, 0))
                except (ValueError, TypeError):
                    error = {
                        'record': metric,
                        'error': f"Invalid numeric value for {field}: {metric.get(field)}"
                    }
                    validation_errors.append(error)
                    continue

        # Add metric to valid list if it passes all validations
        valid_metrics.append(metric)

    logger.info(f"Validated {len(valid_metrics)} video metrics, found {len(validation_errors)} errors")
    return valid_metrics, validation_errors


def validate_search_trends(trends_data):
    """
    Validate search trends data.

    Args:
        trends_data (list): List of transformed search trends

    Returns:
        tuple: (valid_trends, validation_errors)
    """
    valid_trends = []
    validation_errors = []

    for trend in trends_data:
        # Required fields
        required_fields = ['keyword', 'date', 'interest_score']
        missing_fields = [field for field in required_fields if trend.get(field) is None]

        if missing_fields:
            error = {
                'record': trend,
                'error': f"Missing required fields: {', '.join(missing_fields)}"
            }
            validation_errors.append(error)
            continue

        # Validate date format
        if trend.get('date'):
            try:
                datetime.strptime(trend.get('date'), '%Y-%m-%d')
            except ValueError:
                error = {
                    'record': trend,
                    'error': f"Invalid date format: {trend.get('date')}"
                }
                validation_errors.append(error)
                continue

        # Validate interest score
        if not isinstance(trend.get('interest_score'), (int, float)):
            try:
                trend['interest_score'] = int(trend.get('interest_score', 0))
            except (ValueError, TypeError):
                error = {
                    'record': trend,
                    'error': f"Invalid interest score: {trend.get('interest_score')}"
                }
                validation_errors.append(error)
                continue

        # Add trend to valid list if it passes all validations
        valid_trends.append(trend)

    logger.info(f"Validated {len(valid_trends)} search trends, found {len(validation_errors)} errors")
    return valid_trends, validation_errors


def validate_integrated_analysis(analysis_data):
    """
    Validate integrated analysis data.

    Args:
        analysis_data (list): List of integrated analysis records

    Returns:
        tuple: (valid_analyses, validation_errors)
    """
    valid_analyses = []
    validation_errors = []

    for analysis in analysis_data:
        # Required fields
        required_fields = ['event_id', 'event_date', 'event_name']
        missing_fields = [field for field in required_fields if not analysis.get(field)]

        if missing_fields:
            error = {
                'record': analysis,
                'error': f"Missing required fields: {', '.join(missing_fields)}"
            }
            validation_errors.append(error)
            continue

        # Validate date format
        if analysis.get('event_date'):
            try:
                datetime.strptime(analysis.get('event_date'), '%Y-%m-%d')
            except ValueError:
                error = {
                    'record': analysis,
                    'error': f"Invalid date format: {analysis.get('event_date')}"
                }
                validation_errors.append(error)
                continue

        # Validate JSON fields
        json_fields = ['related_videos', 'search_metrics']
        for field in json_fields:
            if analysis.get(field):
                try:
                    json.loads(analysis.get(field))
                except json.JSONDecodeError:
                    error = {
                        'record': analysis,
                        'error': f"Invalid JSON in field {field}: {analysis.get(field)}"
                    }
                    validation_errors.append(error)
                    continue

        # Add analysis to valid list if it passes all validations
        valid_analyses.append(analysis)

    logger.info(f"Validated {len(valid_analyses)} integrated analyses, found {len(validation_errors)} errors")
    return valid_analyses, validation_errors