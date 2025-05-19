"""
Transformation module for Google Trends data.
Converts raw Google Trends data into structured formats for BigQuery tables.
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


def transform_search_trends(interest_over_time_data, related_event_id=None, related_team_id=None):
    """
    Transform interest over time data into search trends format.

    Args:
        interest_over_time_data (DataFrame): DataFrame from Google Trends API
        related_event_id (str, optional): ID of related event
        related_team_id (str, optional): ID of related team

    Returns:
        list: List of dictionaries with transformed search trends data
    """
    transformed_trends = []

    if interest_over_time_data.empty:
        logger.warning("No interest over time data to transform")
        return transformed_trends

    # Reset index to get the date as a column
    interest_df = interest_over_time_data.reset_index()

    # Convert to dictionary format
    try:
        for _, row in interest_df.iterrows():
            date_val = row.get('date')

            # Skip the isPartial column
            for column in interest_df.columns:
                if column in ['date', 'isPartial']:
                    continue

                # Get the interest score for this keyword
                interest_score = row.get(column)

                if date_val and interest_score is not None:
                    # Convert date to string if it's a datetime object
                    if isinstance(date_val, datetime):
                        date_str = date_val.date().isoformat()
                    else:
                        date_str = date_val.isoformat()

                    transformed_trend = {
                        'keyword': column,
                        'date': date_str,
                        'interest_score': int(interest_score),
                        'related_event_id': related_event_id,
                        'related_team_id': related_team_id,
                        'created_at': datetime.now().isoformat()
                    }

                    transformed_trends.append(transformed_trend)
    except Exception as e:
        logger.error(f"Error transforming interest over time data: {str(e)}")

    logger.info(f"Transformed {len(transformed_trends)} search trend records")
    return transformed_trends


def transform_regional_interest(interest_by_region_data, date, related_event_id=None, related_team_id=None):
    """
    Transform interest by region data into regional interest format.

    Args:
        interest_by_region_data (DataFrame): DataFrame from Google Trends API
        date (str): Date for the regional interest data
        related_event_id (str, optional): ID of related event
        related_team_id (str, optional): ID of related team

    Returns:
        list: List of dictionaries with transformed regional interest data
    """
    transformed_regional = []

    if interest_by_region_data.empty:
        logger.warning("No interest by region data to transform")
        return transformed_regional

    # Convert to dictionary format
    try:
        for region, row in interest_by_region_data.iterrows():
            for column in interest_by_region_data.columns:
                # Get the interest score for this keyword
                interest_score = row.get(column)

                if region and interest_score is not None:
                    transformed_regional_interest = {
                        'keyword': column,
                        'region': region,
                        'date': date,
                        'interest_score': int(interest_score),
                        'related_event_id': related_event_id,
                        'related_team_id': related_team_id,
                        'created_at': datetime.now().isoformat()
                    }

                    transformed_regional.append(transformed_regional_interest)
    except Exception as e:
        logger.error(f"Error transforming interest by region data: {str(e)}")

    logger.info(f"Transformed {len(transformed_regional)} regional interest records")
    return transformed_regional


def transform_related_queries(related_queries_data, date):
    """
    Transform related queries data.

    Args:
        related_queries_data (dict): Dictionary from Google Trends API
        date (str): Date for the related queries data

    Returns:
        list: List of dictionaries with transformed related queries data
    """
    transformed_queries = []

    if not related_queries_data:
        logger.warning("No related queries data to transform")
        return transformed_queries

    try:
        for main_keyword, queries in related_queries_data.items():
            # Process top queries
            if 'top' in queries and queries['top'] is not None and not queries['top'].empty:
                for _, row in queries['top'].iterrows():
                    query = row.get('query')
                    value = row.get('value')

                    if query and value is not None:
                        transformed_query = {
                            'main_keyword': main_keyword,
                            'related_query': query,
                            'date': date,
                            'query_type': 'top',
                            'interest_value': int(value),
                            'created_at': datetime.now().isoformat()
                        }

                        transformed_queries.append(transformed_query)

            # Process rising queries
            if 'rising' in queries and queries['rising'] is not None and not queries['rising'].empty:
                for _, row in queries['rising'].iterrows():
                    query = row.get('query')
                    value = row.get('value')

                    if query and value is not None:
                        # Convert percentage string to integer if necessary
                        if isinstance(value, str) and value.endswith('%'):
                            try:
                                value = int(value.rstrip('%'))
                            except ValueError:
                                value = 0

                        transformed_query = {
                            'main_keyword': main_keyword,
                            'related_query': query,
                            'date': date,
                            'query_type': 'rising',
                            'interest_value': value,
                            'created_at': datetime.now().isoformat()
                        }

                        transformed_queries.append(transformed_query)
    except Exception as e:
        logger.error(f"Error transforming related queries data: {str(e)}")

    logger.info(f"Transformed {len(transformed_queries)} related queries records")
    return transformed_queries