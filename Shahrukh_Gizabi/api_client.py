import requests
import time 

def fetch_api_football_team_stats(api_key, league_id="39", season="2023", team_id="33"):
    url = "https://v3.football.api-sports.io/teams/statistics"
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io", 
        'x-rapidapi-key': api_key
    }
    params = {'league': league_id, 'season': season, 'team': team_id}
    print(f"Fetching team stats from API-Football for team {team_id}, league {league_id}, season {season}")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30) 
        response.raise_for_status() 
        data = response.json()
        
        api_response_content = data.get('response')
        if isinstance(api_response_content, list):
            return api_response_content[0] if api_response_content else None 
        return api_response_content # Or return the dict directly
    except requests.exceptions.Timeout:
        print(f"Timeout error fetching data from api-football.com for team {team_id}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from api-football.com: {e}")
        if 'response' in locals() and response is not None:
            print(f"Response status: {response.status_code}")
            print(f"Response content: {response.text}")
        return None

def fetch_football_data_matches(api_token, competition_code="PL", season="2023"):
    url = f"https://api.football-data.org/v4/competitions/{competition_code}/matches"
    headers = {'X-Auth-Token': api_token}
    params = {'season': season}
    print(f"Fetching matches from football-data.org for {competition_code}, season {season}")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()  
    except requests.exceptions.Timeout:
        print(f"Timeout error fetching data from football-data.org for {competition_code} season {season}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from football-data.org: {e}")
        if 'response' in locals() and response is not None:
            print(f"Response status: {response.status_code}")
            print(f"Response content: {response.text}")
        return None

def fetch_sports_game_odds(api_key, league_id="EPL", starts_after="2024-08-16", starts_before="2025-05-25"):
   
    url = "https://api.sportsgameodds.com/v2/events/"
    params = {
        'apiKey': api_key,
        'leagueID': league_id,
        'startsAfter': starts_after,
        'startsBefore': starts_before
    }
    print(f"Fetching odds from sportsgameodds.com for {league_id} between {starts_after} and {starts_before}")
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        print(f"Timeout error fetching odds from sportsgameodds.com for {league_id}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from sportsgameodds.com: {e}")
        if 'response' in locals() and response is not None:
            print(f"Response status: {response.status_code}")
            print(f"Response content: {response.text}")
        return None