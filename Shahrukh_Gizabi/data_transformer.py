import pandas as pd
import json 

def _clean_invalid_json_chars(text_or_data):
    """Helper function to remove invalid control characters from strings within data if necessary."""
    if isinstance(text_or_data, str):

        return "".join(c for c in text_or_data if c.isprintable() or c in ('\t', '\n', '\r'))
    elif isinstance(text_or_data, list):
        return [_clean_invalid_json_chars(item) for item in text_or_data]
    elif isinstance(text_or_data, dict):
        return {k: _clean_invalid_json_chars(v) for k, v in text_or_data.items()}
    return text_or_data

def _to_json_string(data_object):
    """ Safely converts a Python object to a JSON string, cleaning if necessary. """
    if data_object is None:
        return None
    try:
        return json.dumps(data_object)
    except TypeError as e:
        print(f"Initial json.dumps failed: {e}. Attempting to clean data.")
        cleaned_data = _clean_invalid_json_chars(data_object)
        try:
            return json.dumps(cleaned_data)
        except Exception as final_e:
            print(f"json.dumps failed even after cleaning: {final_e}. Returning None for this field.")
            return None


def transform_team_season_stats_to_df(api_response_data):
    if not api_response_data:
        print("No data provided to transform_team_season_stats_to_df.")
        return pd.DataFrame()

    if isinstance(api_response_data, list):
        if not api_response_data:
            print("Empty list provided to transform_team_season_stats_to_df.")
            return pd.DataFrame()
        data = api_response_data[0] if api_response_data else {} # Assuming the first element is the relevant dict
    elif isinstance(api_response_data, dict):
        data = api_response_data
    else:
        print(f"Unexpected data type for team season stats: {type(api_response_data)}")
        return pd.DataFrame()

    if not data: # If data became an empty dict
        print("Empty data object for team season stats transformation.")
        return pd.DataFrame()
        
    record = {}
    record['form'] = data.get('form')
    record['league'] = _to_json_string(data.get('league'))
    record['team'] = _to_json_string(data.get('team'))
    record['fixtures'] = _to_json_string(data.get('fixtures'))
    
    # Helper for processing minute/under_over keys (as designed in DDL)
    def _process_detailed_stats_keys(stats_data, type_prefix=""):
        if not stats_data: return None
        processed = {}
        for key, val in stats_data.items():
            new_key = key.replace('-', '_').replace('.', '_')
            if type_prefix == "minute_":
                if new_key == "0_15": new_key = "min_0_15"
                elif new_key == "16_30": new_key = "min_16_30"
                elif new_key == "31_45": new_key = "min_31_45"
                elif new_key == "46_60": new_key = "min_46_60"
                elif new_key == "61_75": new_key = "min_61_75"
                elif new_key == "76_90": new_key = "min_76_90"
                elif new_key == "91_105": new_key = "min_91_105"
                elif new_key == "106_120": new_key = "min_106_120"
                elif new_key == "": new_key = "unknown_time" # Esp. for cards
            elif type_prefix == "under_over_":
                new_key = f"under_over_{new_key}"
            processed[new_key] = val
        return processed

    goals_data = data.get('goals', {})
    if goals_data:
        processed_goals = {}
        for goal_aspect in ['for', 'against']:
            if goal_aspect in goals_data and isinstance(goals_data[goal_aspect], dict):
                aspect_data = goals_data[goal_aspect]
                processed_goals[goal_aspect] = {
                    'total': aspect_data.get('total'),
                    'average': aspect_data.get('average'),
                    'minute': _process_detailed_stats_keys(aspect_data.get('minute'), "minute_"),
                    'under_over': _process_detailed_stats_keys(aspect_data.get('under_over'), "under_over_")
                }
        record['goals'] = _to_json_string(processed_goals)
    else:
        record['goals'] = None
        
    record['biggest'] = _to_json_string(data.get('biggest'))
    record['clean_sheet'] = _to_json_string(data.get('clean_sheet'))
    record['failed_to_score'] = _to_json_string(data.get('failed_to_score'))
    record['penalty'] = _to_json_string(data.get('penalty'))
    record['lineups'] = _to_json_string(data.get('lineups'))
    
    cards_data = data.get('cards', {})
    if cards_data:
        processed_cards = {}
        if 'yellow' in cards_data:
            processed_cards['yellow'] = _process_detailed_stats_keys(cards_data.get('yellow'), "minute_")
        if 'red' in cards_data:
            processed_cards['red'] = _process_detailed_stats_keys(cards_data.get('red'), "minute_")
        record['cards'] = _to_json_string(processed_cards)
    else:
        record['cards'] = None
        
    return pd.DataFrame([record])


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
        processed_matches.append(record)
        
    return pd.DataFrame(processed_matches)


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
        processed_events_records.append(record)
        
    return pd.DataFrame(processed_events_records)