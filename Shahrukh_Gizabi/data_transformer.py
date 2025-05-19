# data_transformer.py
import pandas as pd
import json
import datetime # Added for the ingested_at timestamp

# --- Helper Function ---
def _to_json_string(data_object):
    """ Safely converts a Python object to a JSON string, handling potential errors. """
    if data_object is None:
        return None
    try:
        return json.dumps(data_object)
    except TypeError as e:
        print(f"Warning: TypeError during json.dumps: {e}. Data: {str(data_object)[:1000]}. Returning None.")
        return None
    except Exception as e:
        print(f"Warning: Unexpected error during json.dumps: {e}. Data: {str(data_object)[:1000]}. Returning None.")
        return None

# --- Transformation Function for TeamSeasonStats ---
def transform_team_season_stats_to_df(api_response_data):
    """
    Transforms the JSON response for team season stats (from api-football)
    into a Pandas DataFrame, preparing complex types as JSON strings for BigQuery,
    using ARRAY<STRUCT<...>> for minute, under_over, and card stats.
    """
    if not api_response_data:
        print("No data provided to transform_team_season_stats_to_df.")
        return pd.DataFrame()

    if isinstance(api_response_data, list):
        data = api_response_data[0] if api_response_data else {}
    elif isinstance(api_response_data, dict):
        data = api_response_data
    else:
        print(f"Unexpected data type for team season stats: {type(api_response_data)}")
        return pd.DataFrame()

    if not data:
        print("Empty data object for team season stats transformation.")
        return pd.DataFrame()
        
    record = {}
    record['form'] = data.get('form')
    record['league'] = _to_json_string(data.get('league'))
    record['team'] = _to_json_string(data.get('team'))
    record['fixtures'] = _to_json_string(data.get('fixtures'))
    
    def _create_segment_stats_array(segment_data_dict):
        if not isinstance(segment_data_dict, dict): return []
        array = []
        for segment_key, stats_values in segment_data_dict.items():
            if isinstance(stats_values, dict):
                array.append({
                    "segment": segment_key if segment_key else "unknown_time",
                    "total": stats_values.get("total"),
                    "percentage": stats_values.get("percentage")
                })
        return array

    def _create_under_over_stats_array(under_over_data_dict):
        if not isinstance(under_over_data_dict, dict): return []
        array = []
        for line_key, values_dict in under_over_data_dict.items():
            if isinstance(values_dict, dict):
                array.append({
                    "line": line_key,
                    "over_val": values_dict.get("over"),
                    "under_val": values_dict.get("under")
                })
        return array

    goals_data_raw = data.get('goals', {})
    processed_goals = {}
    if isinstance(goals_data_raw, dict):
        for aspect in ['for', 'against']:
            aspect_data_raw = goals_data_raw.get(aspect)
            if isinstance(aspect_data_raw, dict):
                processed_aspect = {
                    'total': aspect_data_raw.get('total'),
                    'average': aspect_data_raw.get('average'),
                    'minute_stats': _create_segment_stats_array(aspect_data_raw.get('minute')),
                    'under_over_stats': _create_under_over_stats_array(aspect_data_raw.get('under_over'))
                }
                processed_goals[aspect] = processed_aspect
    record['goals'] = _to_json_string(processed_goals) if processed_goals else None
        
    record['biggest'] = _to_json_string(data.get('biggest'))
    record['clean_sheet'] = _to_json_string(data.get('clean_sheet'))
    record['failed_to_score'] = _to_json_string(data.get('failed_to_score'))
    record['penalty'] = _to_json_string(data.get('penalty'))
    record['lineups'] = _to_json_string(data.get('lineups'))
    
    cards_data_raw = data.get('cards', {})
    processed_cards = {}
    if isinstance(cards_data_raw, dict):
        if 'yellow' in cards_data_raw:
            processed_cards['yellow_card_stats'] = _create_segment_stats_array(cards_data_raw.get('yellow'))
        if 'red' in cards_data_raw:
            processed_cards['red_card_stats'] = _create_segment_stats_array(cards_data_raw.get('red'))
    record['cards'] = _to_json_string(processed_cards) if processed_cards else None
        
    record['last_updated'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    df = pd.DataFrame([record])
    return df

# --- Transformation Function for Matches ---
def transform_matches_to_df(api_response_data):
    if not api_response_data or not isinstance(api_response_data.get('matches'), list):
        print("No 'matches' data found or not a list in transform_matches_to_df.")
        return pd.DataFrame()

    matches_list_raw = api_response_data.get('matches', [])
    processed_matches = []

    for match_data in matches_list_raw:
        if not isinstance(match_data, dict):
            print(f"Skipping a match_data item as it's not a dictionary: {match_data}")
            continue
        record = {}
        record['match_id'] = match_data.get('id')
        record['utcDate'] = match_data.get('utcDate')
        record['status'] = match_data.get('status')
        record['matchday'] = match_data.get('matchday')
        record['stage'] = match_data.get('stage')
        record['group'] = match_data.get('group')
        record['lastUpdated'] = match_data.get('lastUpdated')
        record['area'] = _to_json_string(match_data.get('area'))
        record['competition'] = _to_json_string(match_data.get('competition'))
        record['season'] = _to_json_string(match_data.get('season'))
        record['homeTeam'] = _to_json_string(match_data.get('homeTeam'))
        record['awayTeam'] = _to_json_string(match_data.get('awayTeam'))
        record['score'] = _to_json_string(match_data.get('score'))
        record['referees'] = _to_json_string(match_data.get('referees'))
        # Add ingested_at if this table needs it
        # record['ingested_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        processed_matches.append(record)
        
    df = pd.DataFrame(processed_matches)

    if not df.empty:
         df['ingested_at'] = pd.Timestamp.now(tz='UTC').isoformat()
    return df

# --- Transformation Function for MatchEventsAndOdds ---
def transform_match_events_odds_to_df(api_response_data):
    if not api_response_data or not isinstance(api_response_data.get('data'), list):
        print("No 'data' (events list) found or not a list in sportsgameodds response for transform_match_events_odds_to_df.")
        return pd.DataFrame()

    events_list_raw = api_response_data.get('data', [])
    processed_events_records = []

    for event_data in events_list_raw:
        if not isinstance(event_data, dict):
            print(f"Skipping an event_data item as it's not a dictionary: {event_data}")
            continue
        record = {}
        record['eventID'] = event_data.get('eventID')
        record['sportID'] = event_data.get('sportID')
        record['leagueID'] = event_data.get('leagueID')
        record['type'] = event_data.get('type')
        record['info'] = _to_json_string(event_data.get('info'))

        players_object_raw = event_data.get('players', {})
        event_players_list = []
        if isinstance(players_object_raw, dict):
            for player_key, player_details_dict in players_object_raw.items():
                if isinstance(player_details_dict, dict):
                    event_players_list.append({
                        'player_key': player_key,
                        'playerID': player_details_dict.get('playerID'),
                        'name': player_details_dict.get('name'),
                        'teamID': player_details_dict.get('teamID'),
                        'alias': player_details_dict.get('alias'),
                        'firstName': player_details_dict.get('firstName'),
                        'lastName': player_details_dict.get('lastName'),
                        'nickname': player_details_dict.get('nickname')
                    })
        record['event_players'] = _to_json_string(event_players_list) if event_players_list else None
        
        record['teams'] = _to_json_string(event_data.get('teams'))

        results_data_raw = event_data.get('results', {})
        processed_results_struct = {}
        if isinstance(results_data_raw, dict):
            processed_results_struct['res_1h'] = results_data_raw.get('1h')
            processed_results_struct['res_2h'] = results_data_raw.get('2h')
            processed_results_struct['reg'] = results_data_raw.get('reg')
            
            game_results_raw = results_data_raw.get('game', {})
            processed_game_team_aggregates = {}
            player_game_stats_list = []
            if isinstance(game_results_raw, dict):
                processed_game_team_aggregates['home'] = game_results_raw.get('home')
                processed_game_team_aggregates['away'] = game_results_raw.get('away')
                for key, stats_dict in game_results_raw.items():
                    if key not in ['home', 'away'] and isinstance(stats_dict, dict):
                        player_stat_entry = stats_dict.copy()
                        player_stat_entry['player_key'] = key
                        player_game_stats_list.append(player_stat_entry)
            processed_results_struct['game'] = processed_game_team_aggregates
            processed_results_struct['player_game_stats'] = player_game_stats_list
        record['results'] = _to_json_string(processed_results_struct) if processed_results_struct else None

        odds_object_raw = event_data.get('odds', {})
        markets_list = []
        if isinstance(odds_object_raw, dict):
            for market_key, market_details_raw in odds_object_raw.items():
                if isinstance(market_details_raw, dict):
                    bookmakers_list = []
                    by_bookmaker_object_raw = market_details_raw.get('byBookmaker', {})
                    if isinstance(by_bookmaker_object_raw, dict):
                        for bk_name, bk_details_dict in by_bookmaker_object_raw.items():
                            if isinstance(bk_details_dict, dict):
                                bookmakers_list.append({
                                    'bookmaker_name': bk_name,
                                    'odds': bk_details_dict.get('odds'),
                                    'over_under_line': bk_details_dict.get('overUnder'),
                                    'spread': bk_details_dict.get('spread'),
                                    'available': bk_details_dict.get('available'),
                                    'is_main_line': bk_details_dict.get('isMainLine'),
                                    'last_updated_at': bk_details_dict.get('lastUpdatedAt')
                                })
                    markets_list.append({
                        'market_key': market_key,
                        'oddID': market_details_raw.get('oddID'),
                        'opposingOddID': market_details_raw.get('opposingOddID'),
                        'marketName': market_details_raw.get('marketName'),
                        'statID': market_details_raw.get('statID'),
                        'statEntityID': market_details_raw.get('statEntityID'),
                        'periodID': market_details_raw.get('periodID'),
                        'betTypeID': market_details_raw.get('betTypeID'),
                        'sideID': market_details_raw.get('sideID'),
                        'playerID': market_details_raw.get('playerID'),
                        'started': market_details_raw.get('started'),
                        'ended': market_details_raw.get('ended'),
                        'cancelled': market_details_raw.get('cancelled'),
                        'bookOddsAvailable': market_details_raw.get('bookOddsAvailable'),
                        'fairOddsAvailable': market_details_raw.get('fairOddsAvailable'),
                        'fairOdds': market_details_raw.get('fairOdds'),
                        'bookOdds': market_details_raw.get('bookOdds'),
                        'fairOverUnder': market_details_raw.get('fairOverUnder'),
                        'bookOverUnder': market_details_raw.get('bookOverUnder'),
                        'fairSpread': market_details_raw.get('fairSpread'),
                        'bookSpread': market_details_raw.get('bookSpread'),
                        'score': market_details_raw.get('score'),
                        'scoringSupported': market_details_raw.get('scoringSupported'),
                        'bookmakers': bookmakers_list
                    })
        record['markets'] = _to_json_string(markets_list) if markets_list else None
        
        # Add ingested_at for MatchEventsAndOdds table
        record['ingested_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        processed_events_records.append(record)
        
    df = pd.DataFrame(processed_events_records)
    # No need to add 'ingested_at' again here if it was added to each record
    return df