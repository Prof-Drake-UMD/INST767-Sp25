CREATE TABLE soccer_premier_league_analytics.Matches (
    area STRUCT<
        id INT64,
        name STRING,
        code STRING,
        flag STRING
    >,
    competition STRUCT<
        id INT64,
        name STRING,
        code STRING,
        type STRING,
        emblem STRING
    >,
    season STRUCT<
        id INT64,
        startDate DATE,
        endDate DATE,
        currentMatchday INT64,
        winner STRUCT< -- Assuming winner here refers to the competition winner, populated at season end
            id INT64, -- This could be a team ID
            name STRING,
            shortName STRING,
            tla STRING,
            crest STRING
        >
    >,
    match_id INT64, -- Renamed from 'id' in JSON to be more descriptive
    utcDate TIMESTAMP,
    status STRING,
    matchday INT64,
    stage STRING,
    `group` STRING, -- 'group' is a reserved keyword, so it's quoted
    lastUpdated TIMESTAMP,
    homeTeam STRUCT<
        id INT64,
        name STRING,
        shortName STRING,
        tla STRING,
        crest STRING
    >,
    awayTeam STRUCT<
        id INT64,
        name STRING,
        shortName STRING,
        tla STRING,
        crest STRING
    >,
    score STRUCT<
        winner STRING, -- e.g., 'HOME_TEAM', 'AWAY_TEAM', 'DRAW'
        duration STRING, -- e.g., 'REGULAR'
        fullTime STRUCT<home INT64, away INT64>,
        halfTime STRUCT<home INT64, away INT64>
    >,
    -- The 'odds' field in your snippet just contained a message,
    -- so we won't define a complex structure for it here from this source.
    -- Actual odds will come from your third API.
    -- We can add a simple string field if you want to store that message, or omit it.
    -- For now, I'll omit it to avoid confusion with the real odds table.
    referees ARRAY<
        STRUCT<
            id INT64,
            name STRING,
            type STRING, -- e.g., 'REFEREE'
            nationality STRING
        >
    >,
    -- Add a field to record when this data was ingested/updated
    ingested_at TIMESTAMP
);