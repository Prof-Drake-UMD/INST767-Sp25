CREATE OR REPLACE TABLE soccer_premier_league_analytics.TeamSeasonStats (
    league STRUCT<
        id INT64,
        name STRING,
        country STRING,
        logo STRING,
        flag STRING,
        season INT64
    >,
    team STRUCT<
        id INT64,
        name STRING,
        logo STRING
    >,
    form STRING,
    fixtures STRUCT<
        played STRUCT<home INT64, away INT64, total INT64>,
        wins STRUCT<home INT64, away INT64, total INT64>,
        draws STRUCT<home INT64, away INT64, total INT64>,
        loses STRUCT<home INT64, away INT64, total INT64>
    >,
    goals STRUCT<
        `for` STRUCT<
            total STRUCT<home INT64, away INT64, total INT64>,
            average STRUCT<home STRING, away STRING, total STRING>,
            minute_stats ARRAY< 
                STRUCT<
                    segment STRING, 
                    total INT64,
                    percentage STRING
                >
            >,
            under_over_stats ARRAY< 
                STRUCT<
                    line STRING,   
                    over_val INT64, 
                    under_val INT64
                >
            >
        >,
        against STRUCT<
            total STRUCT<home INT64, away INT64, total INT64>,
            average STRUCT<home STRING, away STRING, total STRING>, 
            minute_stats ARRAY< 
                STRUCT<
                    segment STRING,
                    total INT64,
                    percentage STRING
                >
            >,
            under_over_stats ARRAY< 
                STRUCT<
                    line STRING,
                    over_val INT64,
                    under_val INT64
                >
            >
        >
    >,
    biggest STRUCT<
        streak STRUCT<wins INT64, draws INT64, loses INT64>,
        wins STRUCT<home STRING, away STRING>, 
        loses STRUCT<home STRING, away STRING>, 
        goals STRUCT<
            `for` STRUCT<home INT64, away INT64>, 
            against STRUCT<home INT64, away INT64>
        >
    >,
    clean_sheet STRUCT<home INT64, away INT64, total INT64>,
    failed_to_score STRUCT<home INT64, away INT64, total INT64>,
    penalty STRUCT<
        scored STRUCT<total INT64, percentage STRING>,
        missed STRUCT<total INT64, percentage STRING>,
        total INT64
    >,
    lineups ARRAY<
        STRUCT<formation STRING, played INT64>
    >,
    cards STRUCT<
        yellow_card_stats ARRAY< 
            STRUCT<
                segment STRING, 
                total INT64,
                percentage STRING
            >
        >,
        red_card_stats ARRAY< 
            STRUCT<
                segment STRING,
                total INT64,
                percentage STRING
            >
        >
    >,
    last_updated TIMESTAMP
);