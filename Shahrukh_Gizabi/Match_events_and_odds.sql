CREATE OR REPLACE TABLE soccer_premier_league_analytics.MatchEventsAndOdds (
    eventID STRING,
    sportID STRING,
    leagueID STRING,
    type STRING,
    info STRING,                 -- Will hold JSON string for the 'info' object
    event_players STRING,        -- Will hold JSON string for the 'event_players' array of structs
    teams STRING,                -- Will hold JSON string for the 'teams' struct (containing home/away)
    results STRING,              -- Will hold JSON string for the 'results' struct (containing 1h, 2h, reg, game, player_game_stats array)
    markets STRING,              -- Will hold JSON string for the 'markets' array of structs (containing bookmakers array)
    ingested_at TIMESTAMP        -- To be populated by your Cloud Function with the load time
);