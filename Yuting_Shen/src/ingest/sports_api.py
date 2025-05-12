"""
Sports API connector for retrieving sports data.
Uses the TheSportsDB API to retrieve events, teams, and leagues.
"""

import os
import json
import requests
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SportsAPI:
    """
    Client for interacting with the TheSportsDB API.
    Retrieve sports data including events, teams, and leagues.
    """

    def __init__(self):
        """Initialize the SportsAPI client."""
        self.base_url = "https://www.thesportsdb.com/api/v1/json/3"

    def _make_request(self, endpoint, params=None):
        """
        Make a request to the API.

        Args:
            endpoint (str): API endpoint
            params (dict, optional): Query parameters

        Returns:
            dict: JSON response
        """
        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise exception for 4XX/5XX status codes

            # Return the JSON response
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {endpoint}: {str(e)}")
            return {"errors": str(e)}

    def get_league(self, league_id):
        """
        Get information about a specific league.

        Args:
            league_id (str): ID of the league

        Returns:
            dict: JSON response with league data
        """
        response = self._make_request(f"lookupleague.php", {"id": league_id})

        # Save raw data to file with timestamp
        self._save_raw_data(response, f"league_{league_id}")

        return response

    def get_team(self, team_id):
        """
        Get information about a specific team.

        Args:
            team_id (str): ID of the team

        Returns:
            dict: JSON response with team data
        """
        response = self._make_request(f"lookupteam.php", {"id": team_id})

        # Save raw data to file with timestamp
        self._save_raw_data(response, f"team_{team_id}")

        return response

    def get_past_events(self, league_id, limit=15):
        """
        Get past events for a specific league.

        Args:
            league_id (str): ID of the league
            limit (int, optional): Number of events to return

        Returns:
            dict: JSON response with events data
        """
        response = self._make_request(f"eventspastleague.php", {"id": league_id})

        # Save raw data to file with timestamp
        self._save_raw_data(response, f"past_events_league_{league_id}")

        # Limit the number of events if specified
        if "events" in response and isinstance(response["events"], list) and limit:
            response["events"] = response["events"][:limit]

        return response

    def get_event_details(self, event_id):
        """
        Get detailed information about an event.

        Args:
            event_id (str): ID of the event

        Returns:
            dict: JSON response with event details
        """
        response = self._make_request(f"lookupevent.php", {"id": event_id})

        # Save raw data to file with timestamp
        self._save_raw_data(response, f"event_{event_id}")

        return response

    def search_teams(self, team_name):
        """
        Search for teams by name.

        Args:
            team_name (str): Name of the team to search for

        Returns:
            dict: JSON response with search results
        """
        response = self._make_request(f"searchteams.php", {"t": team_name})

        # Save raw data to file with timestamp
        self._save_raw_data(response, f"team_search_{team_name}")

        return response

    def _save_raw_data(self, data, prefix):
        """
        Save raw API response to a file with timestamp.

        Args:
            data (dict): Data to save
            prefix (str): Prefix for the filename
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/raw/sportsdb_{prefix}_{timestamp}.json"

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Write data to file
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved raw data to {filepath}")


# Example usage
if __name__ == "__main__":
    # Initialize the API client
    api = SportsAPI()

    # Example: Get information about English Premier League (ID: 4328)
    league_data = api.get_league("4328")

    # Example: Get past events for English Premier League
    events_data = api.get_past_events("4328", limit=5)

    # Print the first event
    if "events" in events_data and events_data["events"]:
        first_event = events_data["events"][0]
        print(f"Event: {first_event['strEvent']} on {first_event['dateEvent']}")

        # Get details for this event
        event_id = first_event["idEvent"]
        event_details = api.get_event_details(event_id)
        print(f"Retrieved details for event {event_id}")
