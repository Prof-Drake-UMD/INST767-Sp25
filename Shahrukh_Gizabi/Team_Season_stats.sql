CREATE TABLE soccer_premier_league_analytics.TeamSeasonStats (
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
        `for` STRUCT< -- "for" is a reserved keyword, so it's quoted
            total STRUCT<home INT64, away INT64, total INT64>,
            average STRUCT<home STRING, away STRING, total STRING>, -- Stored as STRING, consider FLOAT64 post-ETL
            minute STRUCT<
                min_0_15 STRUCT<total INT64, percentage STRING>,
                min_16_30 STRUCT<total INT64, percentage STRING>,
                min_31_45 STRUCT<total INT64, percentage STRING>,
                min_46_60 STRUCT<total INT64, percentage STRING>,
                min_61_75 STRUCT<total INT64, percentage STRING>,
                min_76_90 STRUCT<total INT64, percentage STRING>,
                min_91_105 STRUCT<total INT64, percentage STRING>,
                min_106_120 STRUCT<total INT64, percentage STRING>
            >,
            under_over STRUCT< -- Field names like "0.5" need to be valid SQL names
                under_over_0_5 STRUCT<over INT64, under INT64>,
                under_over_1_5 STRUCT<over INT64, under INT64>,
                under_over_2_5 STRUCT<over INT64, under INT64>,
                under_over_3_5 STRUCT<over INT64, under INT64>,
                under_over_4_5 STRUCT<over INT64, under INT64>
            >
        >,
        against STRUCT<
            total STRUCT<home INT64, away INT64, total INT64>,
            average STRUCT<home STRING, away STRING, total STRING>, -- Stored as STRING, consider FLOAT64 post-ETL
            minute STRUCT<
                min_0_15 STRUCT<total INT64, percentage STRING>,
                min_16_30 STRUCT<total INT64, percentage STRING>,
                min_31_45 STRUCT<total INT64, percentage STRING>,
                min_46_60 STRUCT<total INT64, percentage STRING>,
                min_61_75 STRUCT<total INT64, percentage STRING>,
                min_76_90 STRUCT<total INT64, percentage STRING>,
                min_91_105 STRUCT<total INT64, percentage STRING>,
                min_106_120 STRUCT<total INT64, percentage STRING>
            >,
             under_over STRUCT<
                under_over_0_5 STRUCT<over INT64, under INT64>,
                under_over_1_5 STRUCT<over INT64, under INT64>,
                under_over_2_5 STRUCT<over INT64, under INT64>,
                under_over_3_5 STRUCT<over INT64, under INT64>,
                under_over_4_5 STRUCT<over INT64, under INT64>
            >
        >
    >,
    biggest STRUCT<
        streak STRUCT<wins INT64, draws INT64, loses INT64>,
        wins STRUCT<home STRING, away STRING>, -- e.g., "3-0"
        loses STRUCT<home STRING, away STRING>, -- e.g., "0-3"
        goals STRUCT<
            `for` STRUCT<home INT64, away INT64>, -- "for" is a reserved keyword
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
        yellow STRUCT<
            min_0_15 STRUCT<total INT64, percentage STRING>,
            min_16_30 STRUCT<total INT64, percentage STRING>,
            min_31_45 STRUCT<total INT64, percentage STRING>,
            min_46_60 STRUCT<total INT64, percentage STRING>,
            min_61_75 STRUCT<total INT64, percentage STRING>,
            min_76_90 STRUCT<total INT64, percentage STRING>,
            min_91_105 STRUCT<total INT64, percentage STRING>,
            min_106_120 STRUCT<total INT64, percentage STRING>,
            unknown_time STRUCT<total INT64, percentage STRING> -- For the empty string key ""
        >,
        red STRUCT<
            min_0_15 STRUCT<total INT64, percentage STRING>,
            min_16_30 STRUCT<total INT64, percentage STRING>,
            min_31_45 STRUCT<total INT64, percentage STRING>,
            min_46_60 STRUCT<total INT64, percentage STRING>,
            min_61_75 STRUCT<total INT64, percentage STRING>,
            min_76_90 STRUCT<total INT64, percentage STRING>,
            min_91_105 STRUCT<total INT64, percentage STRING>,
            min_106_120 STRUCT<total INT64, percentage STRING>
        >
    >,
    -- Add a field to record when this data was ingested/updated
    last_updated TIMESTAMP
);