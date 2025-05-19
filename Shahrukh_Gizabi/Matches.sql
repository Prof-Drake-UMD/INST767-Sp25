CREATE OR REPLACE TABLE soccer_premier_league_analytics.Matches (
    match_id INT64,
    utcDate TIMESTAMP,
    status STRING,
    matchday INT64,
    stage STRING,
    `group` STRING,
    lastUpdated TIMESTAMP,
    area STRING,          -- Was STRUCT, now STRING (will hold JSON string)
    competition STRING,   -- Was STRUCT, now STRING (will hold JSON string)
    season STRING,        -- Was STRUCT, now STRING (will hold JSON string)
    homeTeam STRING,      -- Was STRUCT, now STRING (will hold JSON string)
    awayTeam STRING,      -- Was STRUCT, now STRING (will hold JSON string)
    score STRING,         -- Was STRUCT, now STRING (will hold JSON string)
    referees STRING,      -- Was ARRAY<STRUCT>, now STRING (will hold JSON array string)
    ingested_at TIMESTAMP -- Assuming you want this, ensure Python adds it
);