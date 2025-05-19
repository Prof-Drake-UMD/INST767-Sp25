"""
Integration transformer for combining data from multiple sources.
Creates denormalized data for integrated analysis.
"""

import json
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_integrated_events_analysis(events_data, videos_data, video_metrics_data, search_trends_data):
    """
    Create integrated events analysis by combining data from multiple sources.

    Args:
        events_data (list): List of transformed events
        videos_data (list): List of transformed videos
        video_metrics_data (list): List of transformed video metrics
        search_trends_data (list): List of transformed search trends

    Returns:
        list: List of integrated event analysis records
    """
    integrated_analyses = []

    if not events_data:
        logger.warning("No events data for integration")
        return integrated_analyses

    for event in events_data:
        try:
            event_id = event.get('event_id')
            event_date_str = event.get('event_date')

            if not event_id or not event_date_str:
                logger.warning(f"Skipping event with missing ID or date: {event}")
                continue

            # Parse event date
            try:
                event_date = datetime.strptime(event_date_str, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"Invalid event date format: {event_date_str}")
                continue

            # Find related videos
            related_videos = []
            for video in videos_data:
                video_related_event_ids = json.loads(video.get('related_event_ids') or '[]')
                if event_id in video_related_event_ids:
                    # Find metrics for this video
                    video_id = video.get('video_id')
                    metrics = next((m for m in video_metrics_data if m.get('video_id') == video_id), None)

                    if metrics:
                        related_videos.append({
                            'video_id': video_id,
                            'title': video.get('title'),
                            'published_at': video.get('published_at'),
                            'view_count': metrics.get('view_count', 0),
                            'like_count': metrics.get('like_count', 0),
                            'comment_count': metrics.get('comment_count', 0)
                        })

            # Calculate video metrics
            total_video_views = sum(v.get('view_count', 0) for v in related_videos)
            total_video_likes = sum(v.get('like_count', 0) for v in related_videos)
            total_video_comments = sum(v.get('comment_count', 0) for v in related_videos)
            video_count = len(related_videos)

            # Find search metrics for this event
            # Get 7 days before and after the event
            start_date = event_date - timedelta(days=7)
            end_date = event_date + timedelta(days=7)

            search_metrics = []
            for trend in search_trends_data:
                if trend.get('related_event_id') == event_id or trend.get('related_team_id') in [
                    event.get('home_team_id'), event.get('away_team_id')]:
                    try:
                        trend_date = datetime.strptime(trend.get('date'), '%Y-%m-%d').date()
                        if start_date <= trend_date <= end_date:
                            search_metrics.append({
                                'keyword': trend.get('keyword'),
                                'date': trend.get('date'),
                                'interest_score': trend.get('interest_score', 0)
                            })
                    except (ValueError, TypeError):
                        continue

            # Calculate search metrics
            pre_event_interest = [m.get('interest_score', 0) for m in search_metrics
                                  if m.get('date') and datetime.strptime(m.get('date'), '%Y-%m-%d').date() < event_date]

            post_event_interest = [m.get('interest_score', 0) for m in search_metrics
                                   if
                                   m.get('date') and datetime.strptime(m.get('date'), '%Y-%m-%d').date() > event_date]

            avg_pre_event_interest = sum(pre_event_interest) / len(pre_event_interest) if pre_event_interest else 0
            avg_post_event_interest = sum(post_event_interest) / len(post_event_interest) if post_event_interest else 0

            peak_search_interest = max([m.get('interest_score', 0) for m in search_metrics]) if search_metrics else 0

            # Find peak search date
            peak_search_metrics = [m for m in search_metrics if m.get('interest_score', 0) == peak_search_interest]
            peak_search_date = peak_search_metrics[0].get('date') if peak_search_metrics else None

            # Calculate engagement score (composite metric)
            engagement_score = 0
            if video_count > 0 and peak_search_interest > 0:
                # Normalize search interest to 0-1 scale
                normalized_search = peak_search_interest / 100

                # Normalize video views (log scale)
                import math
                normalized_views = math.log10(
                    total_video_views + 1) / 6  # Assuming 1M views is max (log10(1000000) = 6)

                # Combined score
                engagement_score = normalized_search * 0.5 + normalized_views * 0.5

            # Create integrated analysis record
            integrated_analysis = {
                'event_id': event_id,
                'event_date': event_date_str,
                'event_name': event.get('event_name'),
                'league_id': event.get('league_id'),
                'league_name': event.get('league_name'),
                'home_team_id': event.get('home_team_id'),
                'home_team_name': event.get('home_team_name'),
                'away_team_id': event.get('away_team_id'),
                'away_team_name': event.get('away_team_name'),
                'home_score': event.get('home_score'),
                'away_score': event.get('away_score'),

                # Video metrics
                'related_videos': json.dumps(related_videos),
                'total_video_views': total_video_views,
                'total_video_likes': total_video_likes,
                'total_video_comments': total_video_comments,
                'video_count': video_count,

                # Search metrics
                'search_metrics': json.dumps(search_metrics),
                'avg_search_interest_pre_event': avg_pre_event_interest,
                'avg_search_interest_post_event': avg_post_event_interest,
                'peak_search_interest': peak_search_interest,
                'peak_search_date': peak_search_date,

                # Integrated metrics
                'engagement_score': engagement_score,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }

            integrated_analyses.append(integrated_analysis)

        except Exception as e:
            logger.error(f"Error creating integrated analysis for event {event.get('event_id')}: {str(e)}")

    logger.info(f"Created {len(integrated_analyses)} integrated event analyses")
    return integrated_analyses