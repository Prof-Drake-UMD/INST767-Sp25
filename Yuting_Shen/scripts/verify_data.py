"""
Script to verify output data against BigQuery schema.
Ensures that the generated CSV files match the expected structure.
"""

import os
import sys
import json
import pandas as pd
import glob
from datetime import datetime

# Add the src directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logging_utils import setup_logger

# Schema definitions based on BigQuery tables
SCHEMA_DEFINITIONS = {
    'events': [
        'event_id', 'event_name', 'event_date', 'event_time', 'event_timestamp',
        'league_id', 'league_name', 'home_team_id', 'home_team_name',
        'away_team_id', 'away_team_name', 'home_score', 'away_score',
        'status', 'season', 'round', 'venue', 'country', 'event_metadata',
        'created_at', 'updated_at'
    ],
    'teams': [
        'team_id', 'team_name', 'league_id', 'league_name', 'country',
        'stadium', 'website', 'facebook', 'twitter', 'instagram',
        'description', 'logo_url', 'jersey_url', 'founded', 'team_metadata',
        'created_at', 'updated_at'
    ],
    'youtube_videos': [
        'video_id', 'channel_id', 'channel_title', 'title', 'description',
        'published_at', 'tags', 'duration', 'category_id', 'default_language',
        'content_type', 'sports_related_tags', 'related_event_ids',
        'related_team_ids', 'created_at', 'updated_at'
    ],
    'video_metrics': [
        'video_id', 'snapshot_date', 'view_count', 'like_count', 'dislike_count',
        'favorite_count', 'comment_count', 'engagement_rate', 'daily_views',
        'daily_likes', 'daily_comments', 'created_at'
    ],
    'video_comments': [
        'comment_id', 'video_id', 'author_name', 'text', 'published_at',
        'like_count', 'sentiment_score', 'parent_id', 'created_at'
    ],
    'search_trends': [
        'keyword', 'date', 'interest_score', 'related_event_id',
        'related_team_id', 'created_at'
    ],
    'regional_interest': [
        'keyword', 'region', 'date', 'interest_score', 'related_event_id',
        'related_team_id', 'created_at'
    ],
    'related_queries': [
        'main_keyword', 'related_query', 'date', 'query_type',
        'interest_value', 'created_at'
    ],
    'integrated_events_analysis': [
        'event_id', 'event_date', 'event_name', 'league_id', 'league_name',
        'home_team_id', 'home_team_name', 'away_team_id', 'away_team_name',
        'home_score', 'away_score', 'related_videos', 'total_video_views',
        'total_video_likes', 'total_video_comments', 'video_count',
        'search_metrics', 'avg_search_interest_pre_event',
        'avg_search_interest_post_event', 'peak_search_interest',
        'peak_search_date', 'engagement_score', 'created_at', 'updated_at'
    ]
}


def verify_csv_file(file_path, schema_key):
    """
    Verify a CSV file against the expected schema.

    Args:
        file_path (str): Path to the CSV file
        schema_key (str): Key to look up the schema

    Returns:
        tuple: (is_valid, issues)
    """
    logger.info(f"Verifying {file_path}")

    # Get expected columns
    expected_columns = SCHEMA_DEFINITIONS.get(schema_key)
    if not expected_columns:
        return False, [f"Unknown schema key: {schema_key}"]

    try:
        # Read CSV file
        df = pd.read_csv(file_path)

        # Check columns
        missing_columns = [col for col in expected_columns if col not in df.columns]
        extra_columns = [col for col in df.columns if col not in expected_columns]

        issues = []

        if missing_columns:
            issues.append(f"Missing columns: {', '.join(missing_columns)}")

        if extra_columns:
            issues.append(f"Extra columns: {', '.join(extra_columns)}")

        # Check for empty values in required columns
        for col in expected_columns:
            if col in df.columns and col.endswith('_id') and df[col].isnull().any():
                issues.append(f"Column {col} has NULL values")

        # Check row count
        row_count = len(df)
        if row_count == 0:
            issues.append("CSV file is empty")
        else:
            logger.info(f"  - Row count: {row_count}")

        return len(issues) == 0, issues

    except Exception as e:
        return False, [f"Error verifying CSV: {str(e)}"]


def find_latest_csv_files(data_dir='data/processed'):
    """
    Find the latest CSV file for each data type.

    Args:
        data_dir (str): Directory containing CSV files

    Returns:
        dict: Dictionary mapping data types to file paths
    """
    latest_files = {}

    for schema_key in SCHEMA_DEFINITIONS.keys():
        # Get all matching CSV files
        pattern = os.path.join(data_dir, f"{schema_key}_*.csv")
        files = glob.glob(pattern)

        if files:
            # Sort by modification time (newest first)
            latest_file = max(files, key=os.path.getmtime)
            latest_files[schema_key] = latest_file

    return latest_files


def verify_all_data(data_dir='data/processed'):
    """
    Verify all CSV files in the data directory.

    Args:
        data_dir (str): Directory containing CSV files

    Returns:
        tuple: (success_count, failure_count, results)
    """
    logger.info(f"Verifying data in {data_dir}")

    # Find latest CSV files
    latest_files = find_latest_csv_files(data_dir)

    if not latest_files:
        logger.warning(f"No CSV files found in {data_dir}")
        return 0, 0, {}

    results = {}
    success_count = 0
    failure_count = 0

    for schema_key, file_path in latest_files.items():
        is_valid, issues = verify_csv_file(file_path, schema_key)

        results[schema_key] = {
            'file_path': file_path,
            'is_valid': is_valid,
            'issues': issues
        }

        if is_valid:
            success_count += 1
            logger.info(f"  ✓ {schema_key}: Valid")
        else:
            failure_count += 1
            logger.error(f"  ✗ {schema_key}: Invalid")
            for issue in issues:
                logger.error(f"    - {issue}")

    return success_count, failure_count, results


def main():
    """Main function."""
    global logger
    logger = setup_logger(log_file='data_verification.log')

    logger.info("Starting data verification")

    success_count, failure_count, results = verify_all_data()

    logger.info(f"Verification complete: {success_count} successes, {failure_count} failures")

    # Save verification results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"verification_results_{timestamp}.json"

    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    logger.info(f"Verification results saved to {output_file}")

    return failure_count == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)