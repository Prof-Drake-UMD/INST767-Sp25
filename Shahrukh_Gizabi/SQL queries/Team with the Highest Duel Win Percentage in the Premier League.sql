WITH PlayerMatchDuelStats AS (
    -- Step 1: Extract duel stats for each player in each match, linking them to their teamID for that event.
    SELECT
        meo.eventID,
        ep.teamID AS player_teamID_string, -- e.g., "MANCHESTER_UNITED_EPL"
        pgs.player_key,
        pgs.duels_won,
        pgs.duels_attempted
    FROM
        soccer_premier_league_analytics.MatchEventsAndOdds AS meo,
        UNNEST(meo.event_players) AS ep,
        UNNEST(meo.results.player_game_stats) AS pgs
    WHERE
        meo.leagueID = 'EPL' -- Assuming EPL filter
        AND meo.info.seasonWeek = 'Premier League 24/25' -- Filter for the specific season
        AND ep.player_key = pgs.player_key -- Join player from roster to their stats
        AND pgs.duels_attempted IS NOT NULL AND pgs.duels_won IS NOT NULL -- Ensure data is present
),
TeamMatchDuels AS (
    -- Step 2: Aggregate duel stats per team for each match.
    SELECT
        pms.eventID,
        pms.player_teamID_string, -- This is our team identifier string
        SUM(pms.duels_won) AS match_duels_won,
        SUM(pms.duels_attempted) AS match_duels_attempted
    FROM
        PlayerMatchDuelStats AS pms
    GROUP BY
        pms.eventID,
        pms.player_teamID_string
),
TeamSeasonDuels AS (
    -- Step 3: Aggregate total duels per team over the entire season.
    -- We also need to get a more readable team name if player_teamID_string is just an ID.
    -- The player_teamID_string from event_players IS the descriptive team ID like "MANCHESTER_UNITED_EPL"
    SELECT
        tmd.player_teamID_string AS team_identifier, -- Use this as the team name/identifier
        SUM(tmd.match_duels_won) AS season_total_duels_won,
        SUM(tmd.match_duels_attempted) AS season_total_duels_attempted
    FROM
        TeamMatchDuels AS tmd
    GROUP BY
        tmd.player_teamID_string
)
-- Final Step: Calculate duel win percentage and rank
SELECT
    tsd.team_identifier AS team_name,
    tsd.season_total_duels_won,
    tsd.season_total_duels_attempted,
    ROUND(SAFE_DIVIDE(tsd.season_total_duels_won, tsd.season_total_duels_attempted) * 100, 2) AS duel_win_percentage
FROM
    TeamSeasonDuels AS tsd
WHERE
    tsd.season_total_duels_attempted > 0 -- Ensure we don't divide by zero and only consider teams with duel data
ORDER BY
    duel_win_percentage DESC
-- LIMIT 10; -- To see the top 10 teams, or LIMIT 1 for just the highest