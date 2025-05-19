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

    def __init__(self, api_key=None):
        """
        Initialize the SportsAPI client.

        Args:
            api_key (str, optional): API key for TheSportsDB (needed for premium tier)
        """
        # Default to the free tier (v3) if no API key is provided
        self.api_key = api_key

        if self.api_key:
            # Use premium tier (v1) with API key
            self.base_url = f"https://www.thesportsdb.com/api/v1/json/{self.api_key}"
        else:
            # Use free tier (v3)
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

    def search_teams(self, team_name):
        """
        Search for teams by name.

        Args:
            team_name (str): Name of the team to search for

        Returns:
            dict: JSON response with search results
        """
        response = self._make_request("searchteams.php", {"t": team_name})

        # Save raw data to file with timestamp
        self._save_raw_data(response, f"team_search_{team_name}")

        return response

    def get_past_events(self, league_id, start_date=None, end_date=None, limit=15):
        """
        Get past events for a specific league within a date range.

        Args:
            league_id (str): ID of the league
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            limit (int, optional): Number of events to return

        Returns:
            dict: JSON response with events data
        """
        params = {"id": league_id}

        if start_date and end_date:
            # Some APIs support date range filtering
            params["start_date"] = start_date
            params["end_date"] = end_date
            endpoint = "eventsseason.php"  # API endpoint for season events
        else:
            endpoint = "eventspastleague.php"  # API endpoint for past events

        response = self._make_request(endpoint, params)

        # Save raw data to file with timestamp
        date_suffix = f"_{start_date}_to_{end_date}" if start_date and end_date else ""
        self._save_raw_data(response, f"past_events_league_{league_id}{date_suffix}")

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

    def search_events(self, event_name, season=None):
        """
        Search for events by event name, with optional season filter.

        Args:
            event_name (str): Name of the event to search for (e.g., 'Arsenal_vs_Chelsea')
            season (str, optional): Season to filter by (e.g., '2016-2017')

        Returns:
            dict: JSON response with event search results
        """
        # Build parameters
        params = {"e": event_name}
        if season:
            params["s"] = season

        response = self._make_request("searchevents.php", params)

        # Save raw data to file with timestamp
        prefix = f"event_search_{event_name}"
        if season:
            prefix += f"_season_{season}"
        self._save_raw_data(response, prefix)

        logger.info(f"Searched for events with name '{event_name}'{' for season ' + season if season else ''}")

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
    api = SportsAPI("308460")

    # Example: Get information about NFL (ID: 4391)
    league_data = api.get_league("4391")

    # Example: Get past events for NFL
    events_data = api.get_past_events("4391", limit=5)

    #search_events = api.search_events("Philadelphia Eagles vs Kansas City Chiefs")

    # Print the first event
    if "events" in events_data and events_data["events"]:
        first_event = events_data["events"][0]
        print(f"Event: {first_event['strEvent']} on {first_event['dateEvent']}")

        # Get details for the first event
        event_id = first_event["idEvent"]
        event_details = api.get_event_details(event_id)
        print(f"Retrieved details for event {event_id}")
