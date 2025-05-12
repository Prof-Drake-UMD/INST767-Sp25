CREATE TABLE soccer_premier_league_analytics.MatchEventsAndOdds (
    eventID STRING,
    sportID STRING,
    leagueID STRING,
    type STRING,
    info STRUCT<
        seasonWeek STRING
    >,
    -- 'players' object transformed into an array of structs
    event_players ARRAY<
        STRUCT<
            player_key STRING, -- Original dynamic key, e.g., "MASON_MOUNT_1_EPL"
            playerID STRING,
            name STRING,
            teamID STRING,
            alias STRING,
            firstName STRING,
            lastName STRING,
            nickname STRING
        >
    >,
    teams STRUCT<
        home STRUCT<
            statEntityID STRING,
            names STRUCT<short STRING, medium STRING, long STRING>,
            teamID STRING, -- e.g., "MANCHESTER_UNITED_EPL"
            colors STRUCT<primary STRING, secondary STRING, primaryContrast STRING, secondaryContrast STRING>,
            score INT64
        >,
        away STRUCT<
            statEntityID STRING,
            names STRUCT<short STRING, medium STRING, long STRING>,
            teamID STRING,
            colors STRUCT<primary STRING, secondary STRING, primaryContrast STRING, secondaryContrast STRING>,
            score INT64
        >
    >,
    results STRUCT<
        res_1h STRUCT< -- Renamed from "1h" to be a valid BQ field name prefix
            home STRUCT<
                dribbles_attempted INT64,
                crosses_accurate INT64,
                longBalls_accurate INT64,
                crosses_attempted INT64,
                passes_accurate INT64,
                dribbles_won INT64,
                interceptions INT64,
                points INT64,
                longBalls_attempted INT64,
                shots_onGoal INT64,
                shots_offGoal INT64,
                shots INT64,
                clearances INT64
            >,
            away STRUCT<
                dribbles_attempted INT64,
                crosses_accurate INT64,
                longBalls_accurate INT64,
                crosses_attempted INT64,
                passes_accurate INT64,
                dribbles_won INT64,
                interceptions INT64,
                points INT64,
                longBalls_attempted INT64,
                shots_onGoal INT64,
                shots_offGoal INT64,
                shots INT64,
                clearances INT64
            >
        >,
        res_2h STRUCT< -- Renamed from "2h"
            home STRUCT<
                dribbles_attempted INT64,
                crosses_accurate INT64,
                longBalls_accurate INT64,
                crosses_attempted INT64,
                passes_accurate INT64,
                dribbles_won INT64,
                interceptions INT64,
                points INT64,
                longBalls_attempted INT64,
                shots_onGoal INT64,
                shots_offGoal INT64,
                shots INT64,
                clearances INT64
            >,
            away STRUCT<
                dribbles_attempted INT64,
                crosses_accurate INT64,
                longBalls_accurate INT64,
                crosses_attempted INT64,
                passes_accurate INT64,
                dribbles_won INT64,
                interceptions INT64,
                points INT64,
                longBalls_attempted INT64,
                shots_onGoal INT64,
                shots_offGoal INT64,
                shots INT64,
                clearances INT64
            >
        >,
        reg STRUCT< -- Regulation time results
            home STRUCT<points INT64>,
            away STRUCT<points INT64>
        >,
        game STRUCT< -- Full game team aggregate stats
            home STRUCT<
                points INT64,
                offsides INT64,
                shots_onGoal INT64,
                yellowCards INT64,
                cornerKicks INT64,
                shots_blocked INT64,
                goalie_saves INT64,
                passes_percent FLOAT64, -- Assuming this can be converted
                shots_outsideBox INT64,
                dribbles_attempted INT64,
                possessionPercent FLOAT64, -- Assuming this can be converted
                dribbles_won INT64,
                crosses_accurate INT64,
                longBalls_accurate INT64,
                crosses_attempted INT64,
                shots_offGoal INT64,
                clearances INT64,
                shots INT64,
                interceptions INT64,
                fouls INT64,
                longBalls_attempted INT64,
                shots_insideBox INT64,
                passes_accurate INT64,
                passes_attempted INT64
            >,
            away STRUCT<
                points INT64,
                offsides INT64,
                shots_onGoal INT64,
                yellowCards INT64,
                cornerKicks INT64,
                shots_blocked INT64,
                goalie_saves INT64,
                passes_percent FLOAT64,
                shots_outsideBox INT64,
                dribbles_attempted INT64,
                possessionPercent FLOAT64,
                dribbles_won INT64,
                crosses_accurate INT64,
                longBalls_accurate INT64,
                crosses_attempted INT64,
                shots_offGoal INT64,
                clearances INT64,
                shots INT64,
                interceptions INT64,
                fouls INT64,
                longBalls_attempted INT64,
                shots_insideBox INT64,
                passes_accurate INT64,
                passes_attempted INT64
            >
        >,
        -- Player-specific game stats transformed into an array of structs
        player_game_stats ARRAY<
            STRUCT<
                player_key STRING, -- Original dynamic key, e.g., "SCOTT_MCTOMINAY_1_EPL"
                goalie_goalsAgainst INT64,
                penaltyKicks_made INT64,
                yellowCards INT64,
                penaltyKicks_missed INT64,
                redCards INT64,
                longBalls_attempted INT64,
                longBalls_accurate INT64,
                passes_attempted INT64,
                touches INT64,
                duels_attempted INT64,
                duels_won INT64,
                minutesPlayed INT64,
                playerRating FLOAT64,
                passes_accurate INT64,
                assists INT64,
                shots_offGoal INT64,
                shots INT64,
                foulsDrawn INT64,
                dribbles_attempted INT64,
                dribbles_won INT64,
                fouls INT64,
                defense_dribbles_lost INT64,
                tackles INT64,
                blocks INT64,
                clearances INT64,
                disposessed INT64,
                interceptions INT64,
                offsides INT64,
                crosses_attempted INT64,
                crosses_accurate INT64,
                goalie_penaltyKicksSaved INT64,
                goalie_insideBox_saves INT64,
                goalie_saves INT64
                -- Add other player stats fields as observed in the full data
            >
        >
    >,
    -- 'odds' object transformed into an array of market structs
    markets ARRAY<
        STRUCT<
            market_key STRING, -- Original dynamic key for the market, e.g., "points-away-1h-ou-under"
            oddID STRING,
            opposingOddID STRING,
            marketName STRING,
            statID STRING,
            statEntityID STRING, -- Can be "home", "away", "all", or a player_key
            periodID STRING,
            betTypeID STRING,
            sideID STRING,
            playerID STRING, -- populated if statEntityID refers to a player market
            started BOOLEAN,
            ended BOOLEAN,
            cancelled BOOLEAN,
            bookOddsAvailable BOOLEAN,
            fairOddsAvailable BOOLEAN,
            fairOdds STRING, -- e.g., "-182", "+254"
            bookOdds STRING, -- e.g., "-209"
            fairOverUnder STRING, -- e.g., "0.5"
            bookOverUnder STRING, -- e.g., "0.5"
            fairSpread STRING, -- e.g., "+0.5"
            bookSpread STRING, -- e.g., "+0.5"
            score FLOAT64, -- Actual outcome value relevant to the bet
            scoringSupported BOOLEAN,
            bookmakers ARRAY<
                STRUCT<
                    bookmaker_name STRING, -- e.g., "unknown", "pinnacle", "bovada"
                    odds STRING,
                    over_under_line STRING,
                    spread STRING,
                    available BOOLEAN,
                    is_main_line BOOLEAN, -- Guessed from context, might not exist
                    last_updated_at TIMESTAMP
                >
            >
        >
    >,
    ingested_at TIMESTAMP
);