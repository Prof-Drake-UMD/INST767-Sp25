CREATE OR REPLACE TABLE soccer_premier_league_analytics.MatchEventsAndOdds (
    eventID STRING,
    sportID STRING,
    leagueID STRING,
    type STRING,
    info STRING, -- << CHANGED TO STRING to load the JSON string directly
    event_players ARRAY<
        STRUCT<
            player_key STRING,
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
            teamID STRING,
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
        res_1h STRUCT<
            home STRUCT<dribbles_attempted INT64, crosses_accurate INT64, longBalls_accurate INT64, crosses_attempted INT64, passes_accurate INT64, dribbles_won INT64, interceptions INT64, points INT64, longBalls_attempted INT64, shots_onGoal INT64, shots_offGoal INT64, shots INT64, clearances INT64>,
            away STRUCT<dribbles_attempted INT64, crosses_accurate INT64, longBalls_accurate INT64, crosses_attempted INT64, passes_accurate INT64, dribbles_won INT64, interceptions INT64, points INT64, longBalls_attempted INT64, shots_onGoal INT64, shots_offGoal INT64, shots INT64, clearances INT64>
        >,
        res_2h STRUCT<
            home STRUCT<dribbles_attempted INT64, crosses_accurate INT64, longBalls_accurate INT64, crosses_attempted INT64, passes_accurate INT64, dribbles_won INT64, interceptions INT64, points INT64, longBalls_attempted INT64, shots_onGoal INT64, shots_offGoal INT64, shots INT64, clearances INT64>,
            away STRUCT<dribbles_attempted INT64, crosses_accurate INT64, longBalls_accurate INT64, crosses_attempted INT64, passes_accurate INT64, dribbles_won INT64, interceptions INT64, points INT64, longBalls_attempted INT64, shots_onGoal INT64, shots_offGoal INT64, shots INT64, clearances INT64>
        >,
        reg STRUCT<home STRUCT<points INT64>, away STRUCT<points INT64>>,
        game STRUCT<
            home STRUCT<points INT64, offsides INT64, shots_onGoal INT64, yellowCards INT64, cornerKicks INT64, shots_blocked INT64, goalie_saves INT64, passes_percent FLOAT64, shots_outsideBox INT64, dribbles_attempted INT64, possessionPercent FLOAT64, dribbles_won INT64, crosses_accurate INT64, longBalls_accurate INT64, crosses_attempted INT64, shots_offGoal INT64, clearances INT64, shots INT64, interceptions INT64, fouls INT64, longBalls_attempted INT64, shots_insideBox INT64, passes_accurate INT64, passes_attempted INT64>,
            away STRUCT<points INT64, offsides INT64, shots_onGoal INT64, yellowCards INT64, cornerKicks INT64, shots_blocked INT64, goalie_saves INT64, passes_percent FLOAT64, shots_outsideBox INT64, dribbles_attempted INT64, possessionPercent FLOAT64, dribbles_won INT64, crosses_accurate INT64, longBalls_accurate INT64, crosses_attempted INT64, shots_offGoal INT64, clearances INT64, shots INT64, interceptions INT64, fouls INT64, longBalls_attempted INT64, shots_insideBox INT64, passes_accurate INT64, passes_attempted INT64>
        >,
        player_game_stats ARRAY<
            STRUCT<
                player_key STRING, goalie_goalsAgainst INT64, penaltyKicks_made INT64, yellowCards INT64, penaltyKicks_missed INT64, redCards INT64, longBalls_attempted INT64, longBalls_accurate INT64, passes_attempted INT64, touches INT64, duels_attempted INT64, duels_won INT64, minutesPlayed INT64, playerRating FLOAT64, passes_accurate INT64, assists INT64, shots_offGoal INT64, shots INT64, foulsDrawn INT64, dribbles_attempted INT64, dribbles_won INT64, fouls INT64, defense_dribbles_lost INT64, tackles INT64, blocks INT64, clearances INT64, disposessed INT64, interceptions INT64, offsides INT64, crosses_attempted INT64, crosses_accurate INT64, goalie_penaltyKicksSaved INT64, goalie_insideBox_saves INT64, goalie_saves INT64
            >
        >
    >,
    markets ARRAY<
        STRUCT<
            market_key STRING, oddID STRING, opposingOddID STRING, marketName STRING, statID STRING, statEntityID STRING, periodID STRING, betTypeID STRING, sideID STRING, playerID STRING, started BOOLEAN, ended BOOLEAN, cancelled BOOLEAN, bookOddsAvailable BOOLEAN, fairOddsAvailable BOOLEAN, fairOdds STRING, bookOdds STRING, fairOverUnder STRING, bookOverUnder STRING, fairSpread STRING, bookSpread STRING, score FLOAT64, scoringSupported BOOLEAN,
            bookmakers ARRAY<
                STRUCT<
                    bookmaker_name STRING, odds STRING, over_under_line STRING, spread STRING, available BOOLEAN, is_main_line BOOLEAN, last_updated_at TIMESTAMP
                >
            >
        >
    >,
    ingested_at TIMESTAMP -- This would be set by your cloud function on load
);