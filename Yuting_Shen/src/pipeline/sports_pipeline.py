"""
Pipeline for processing sports data.
"""

import os
import sys
import json
from datetime import datetime
import logging

# Add the src directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Import API modules
from src.ingest.sports_api import SportsAPI
from src.transform.sports_transformer import transform_events, transform_teams
from src.transform.data_validator import validate_events, validate_teams
from src.utils.data_writer import write_to_csv, write_errors_to_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SportsPipeline:
    """Pipeline for processing sports data."""

    def __init__(self, api_key=None):
        """Initialize the pipeline."""
        self.api = SportsAPI(api_key)
        self.events_data = []
        self.teams_data = []

    def fetch_league_data(self, league_id):
        """
        Fetch league data from the API.

        Args:
            league_id (str): ID of the league to fetch

        Returns:
            dict: Raw league data
        """
        logger.info(f"Fetching data for league {league_id}")
        return self.api.get_league(league_id)

    def fetch_events(self, league_id, start_date=None, end_date=None, limit=50):
        """
        Fetch events data from the API with date range support.

        Args:
            league_id (str): ID of the league to fetch events for
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            limit (int): Maximum number of events to fetch

        Returns:
            dict: Raw events data
        """
        logger.info(f"Fetching events for league {league_id} from {start_date} to {end_date}")
        return self.api.get_past_events(league_id, start_date, end_date, limit)

    def fetch_teams(self, team_search=None, team_ids=None):
        """
        Fetch teams data from the API.

        Args:
            team_search (str): Team name to search for
            team_ids (list): List of team IDs to fetch

        Returns:
            list: List of raw team data
        """
        teams_data = []

        if team_search:
            logger.info(f"Searching for teams matching '{team_search}'")
            search_results = self.api.search_teams(team_search)
            if search_results and 'teams' in search_results and search_results['teams']:
                teams_data.extend(search_results['teams'])

        if team_ids:
            for team_id in team_ids:
                logger.info(f"Fetching data for team {team_id}")
                team_data = self.api.get_team(team_id)
                if team_data and 'teams' in team_data and team_data['teams']:
                    teams_data.extend(team_data['teams'])

        return {'teams': teams_data}

    def process_events(self, raw_events_data):
        """
        Process events data through the pipeline.

        Args:
            raw_events_data (dict): Raw events data from the API

        Returns:
            tuple: (transformed_events, csv_path)
        """
        # Transform events data
        transformed_events = transform_events(raw_events_data)

        # Validate transformed data
        valid_events, errors = validate_events(transformed_events)

        # Write validation errors if any
        if errors:
            write_errors_to_json(errors, 'events_validation_errors')

        # Write valid data to CSV
        csv_path = write_to_csv(valid_events, 'events')

        # Store events data for later use
        self.events_data = valid_events

        return valid_events, csv_path

    def process_teams(self, raw_teams_data):
        """
        Process teams data through the pipeline.

        Args:
            raw_teams_data (dict): Raw teams data from the API

        Returns:
            tuple: (transformed_teams, csv_path)
        """
        # Transform teams data
        transformed_teams = transform_teams(raw_teams_data)

        # Validate transformed data
        valid_teams, errors = validate_teams(transformed_teams)

        # Write validation errors if any
        if errors:
            write_errors_to_json(errors, 'teams_validation_errors')

        # Write valid data to CSV
        csv_path = write_to_csv(valid_teams, 'teams')

        # Store teams data for later use
        self.teams_data = valid_teams

        return valid_teams, csv_path

    def run(self, league_id, team_search=None, start_date=None, end_date=None, event_limit=50):
        """
        Run the complete sports data pipeline.

        Args:
            league_id (str): ID of the league to process
            team_search (str, optional): Team name to search for
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            event_limit (int, optional): Maximum number of events to fetch

        Returns:
            dict: Dictionary with results of the pipeline
        """
        results = {
            'start_time': datetime.now().isoformat(),
            'league_id': league_id,
            'status': 'success',
            'errors': [],
            'csv_paths': {}
        }

        try:
            # Fetch league data
            league_data = self.fetch_league_data(league_id)

            # Fetch events data with date range
            events_data = self.fetch_events(league_id, start_date, end_date, event_limit)

            # Process events data
            transformed_events, events_csv_path = self.process_events(events_data)
            results['csv_paths']['events'] = events_csv_path
            results['events_count'] = len(transformed_events)

            # Extract team IDs from events
            team_ids = set()
            for event in transformed_events:
                if event.get('home_team_id'):
                    team_ids.add(event.get('home_team_id'))
                if event.get('away_team_id'):
                    team_ids.add(event.get('away_team_id'))

            # Fetch teams data
            teams_data = self.fetch_teams(team_search, list(team_ids))

            # Process teams data
            transformed_teams, teams_csv_path = self.process_teams(teams_data)
            results['csv_paths']['teams'] = teams_csv_path
            results['teams_count'] = len(transformed_teams)

        except Exception as e:
            logger.error(f"Error in sports pipeline: {str(e)}")
            results['status'] = 'error'
            results['errors'].append(str(e))

        results['end_time'] = datetime.now().isoformat()
        return results

