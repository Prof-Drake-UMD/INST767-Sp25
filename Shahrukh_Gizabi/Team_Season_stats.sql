CREATE OR REPLACE TABLE soccer_premier_league_analytics.TeamSeasonStats (
    form STRING,
    league STRING,            -- Was STRUCT, now STRING (will hold JSON string)
    team STRING,              -- Was STRUCT, now STRING (will hold JSON string)
    fixtures STRING,          -- Was STRUCT, now STRING (will hold JSON string)
    goals STRING,             -- Was STRUCT (containing arrays), now STRING (will hold JSON string)
    biggest STRING,           -- Was STRUCT, now STRING (will hold JSON string)
    clean_sheet STRING,       -- Was STRUCT, now STRING (will hold JSON string)
    failed_to_score STRING,   -- Was STRUCT, now STRING (will hold JSON string)
    penalty STRING,           -- Was STRUCT, now STRING (will hold JSON string)
    lineups STRING,           -- Was ARRAY<STRUCT>, now STRING (will hold JSON array string)
    cards STRING,             -- Was STRUCT (containing arrays), now STRING (will hold JSON string)
    last_updated TIMESTAMP
);