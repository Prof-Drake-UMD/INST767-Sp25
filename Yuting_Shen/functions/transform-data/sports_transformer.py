"""
Transformation module for sports data.
Converts raw TheSportsDB API data into structured formats for BigQuery tables.
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


def transform_events(raw_events_data):
    """
    Transform raw events data into the format for the events table.

    Args:
        raw_events_data (dict): Raw data from TheSportsDB API events endpoint

    Returns:
        list: List of dictionaries with transformed event data
    """
    transformed_events = []

    if not raw_events_data or 'events' not in raw_events_data or not raw_events_data['events']:
        logger.warning("No events data to transform")
        return transformed_events

    for event in raw_events_data['events']:
        try:
            # Extract event date and time
            event_date = event.get('dateEvent')
            event_time = event.get('strTime')
            event_timestamp = None

            if event_date:
                if event_time:
                    # Combine date and time into a timestamp
                    try:
                        datetime_str = f"{event_date} {event_time}"
                        event_timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # Handle alternative time formats
                        try:
                            datetime_str = f"{event_date} {event_time}"
                            event_timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                        except ValueError:
                            logger.warning(f"Could not parse datetime: {datetime_str}")

            # Extract scores
            home_score = None
            away_score = None

            if event.get('intHomeScore') not in (None, ''):
                try:
                    home_score = int(event.get('intHomeScore'))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid home score: {event.get('intHomeScore')}")

            if event.get('intAwayScore') not in (None, ''):
                try:
                    away_score = int(event.get('intAwayScore'))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid away score: {event.get('intAwayScore')}")

            # Construct event metadata
            event_metadata = {
                'sport': event.get('strSport'),
                'circuit': event.get('strCircuit'),
                'description': event.get('strDescriptionEN'),
                'thumb': event.get('strThumb'),
                'banner': event.get('strBanner'),
                'video': event.get('strVideo'),
                'poster': event.get('strPoster')
            }

            # Create transformed event object
            transformed_event = {
                'event_id': event.get('idEvent'),
                'event_name': event.get('strEvent'),
                'event_date': event_date,
                'event_time': event_time,
                'event_timestamp': event_timestamp.isoformat() if event_timestamp else None,
                'league_id': event.get('idLeague'),
                'league_name': event.get('strLeague'),
                'home_team_id': event.get('idHomeTeam'),
                'home_team_name': event.get('strHomeTeam'),
                'away_team_id': event.get('idAwayTeam'),
                'away_team_name': event.get('strAwayTeam'),
                'home_score': home_score,
                'away_score': away_score,
                'status': event.get('strStatus'),
                'season': event.get('strSeason'),
                'round': event.get('strRound'),
                'venue': event.get('strVenue'),
                'country': event.get('strCountry'),
                'event_metadata': json.dumps(event_metadata),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }

            transformed_events.append(transformed_event)

        except Exception as e:
            logger.error(f"Error transforming event {event.get('idEvent')}: {str(e)}")

    logger.info(f"Transformed {len(transformed_events)} events")
    return transformed_events


def transform_teams(raw_teams_data):
    """
    Transform raw teams data into the format for the teams table.

    Args:
        raw_teams_data (dict): Raw data from TheSportsDB API teams endpoint

    Returns:
        list: List of dictionaries with transformed team data
    """
    transformed_teams = []

    if not raw_teams_data or 'teams' not in raw_teams_data or not raw_teams_data['teams']:
        logger.warning("No teams data to transform")
        return transformed_teams

    for team in raw_teams_data['teams']:
        try:
            # Extract founded year
            founded = None
            if team.get('intFormedYear') not in (None, ''):
                try:
                    founded = int(team.get('intFormedYear'))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid formed year: {team.get('intFormedYear')}")

            # Construct team metadata
            team_metadata = {
                'sport': team.get('strSport'),
                'description': team.get('strDescriptionEN'),
                'keywords': team.get('strKeywords'),
                'colors': team.get('strKitColour'),
                'manager': team.get('strManager'),
                'alternate_names': team.get('strAlternate')
            }

            # Create transformed team object
            transformed_team = {
                'team_id': team.get('idTeam'),
                'team_name': team.get('strTeam'),
                'league_id': team.get('idLeague'),
                'league_name': team.get('strLeague'),
                'country': team.get('strCountry'),
                'stadium': team.get('strStadium'),
                'website': team.get('strWebsite'),
                'facebook': team.get('strFacebook'),
                'twitter': team.get('strTwitter'),
                'instagram': team.get('strInstagram'),
                'description': team.get('strDescriptionEN'),
                'logo_url': team.get('strTeamBadge'),
                'jersey_url': team.get('strTeamJersey'),
                'founded': founded,
                'team_metadata': json.dumps(team_metadata),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }

            transformed_teams.append(transformed_team)

        except Exception as e:
            logger.error(f"Error transforming team {team.get('idTeam')}: {str(e)}")

    logger.info(f"Transformed {len(transformed_teams)} teams")
    return transformed_teams