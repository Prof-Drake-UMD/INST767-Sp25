CREATE TABLE soccer_premier_league_analytics.TeamSeasonStats_MinimalTest5 (
    league STRUCT<id INT64, name STRING, country STRING, logo STRING, flag STRING, season INT64>,
    team STRUCT<id INT64, name STRING, logo STRING>,
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
            minute STRUCT<
                min_0_15 STRUCT<total INT64, percentage STRING>,
                min_16_30 STRUCT<total INT64, percentage STRING>,
                min_31_45 STRUCT<total INT64, percentage STRING>,
                min_46_60 STRUCT<total INT64, percentage STRING>,
                min_61_75 STRUCT<total INT64, percentage STRING>,
                min_76_90 STRUCT<total INT64, percentage STRING>, -- CORRECTED to single underscore
                min_91_105 STRUCT<total INT64, percentage STRING>,
                min_106_120 STRUCT<total INT64, percentage STRING>
            >,
            under_over STRUCT< 
                under_over_0_5 STRUCT<over INT64, under INT64>
            >
        >,
        against STRUCT< 
            total STRUCT<home INT64, away INT64, total INT64>
        >
    >
);