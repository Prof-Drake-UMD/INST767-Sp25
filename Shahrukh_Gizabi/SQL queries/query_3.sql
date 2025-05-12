WITH TeamOverallPerformance AS (
    SELECT
        team.id AS team_id,
        team.name AS team_name,
        SAFE_DIVIDE(fixtures.wins.total, fixtures.played.total) AS overall_win_percentage
    FROM
        soccer_premier_league_analytics.TeamSeasonStats
    WHERE
        league.season = 2023 AND league.name = 'Premier League' AND fixtures.played.total > 0
),
MostPlayedFormation AS (
    SELECT
        team.id AS team_id,
        l.formation,
        l.played,
        ROW_NUMBER() OVER (PARTITION BY team.id ORDER BY l.played DESC) as rn
    FROM
        soccer_premier_league_analytics.TeamSeasonStats,
        UNNEST(lineups) AS l -- Unnest the lineups array
    WHERE
        league.season = 2023 AND league.name = 'Premier League'
),
WinsWithMostPlayedFormation AS (
    -- This part is more complex as TeamSeasonStats doesn't directly link wins to formations.
    -- A true analysis would require per-match formation data from the Matches table if available.
    -- For this query, based *only* on TeamSeasonStats, we can only show the most played formation.
    -- A deeper analysis linking it to wins would require a richer dataset or assumptions.
    -- Let's reframe to: "What is the most played formation for top teams and how often was it played?"
    SELECT
        TSS.team.name AS team_name,
        MPF.formation AS most_played_formation,
        MPF.played AS games_with_most_played_formation,
        TSS.fixtures.played.total AS total_matches_played,
        ROUND(SAFE_DIVIDE(MPF.played, TSS.fixtures.played.total) * 100, 2) AS perc_games_with_main_formation,
        TOP.overall_win_percentage
    FROM
        soccer_premier_league_analytics.TeamSeasonStats TSS
    JOIN
        MostPlayedFormation MPF ON TSS.team.id = MPF.team_id AND MPF.rn = 1
    JOIN
        TeamOverallPerformance TOP ON TSS.team.id = TOP.team_id
    WHERE
        TSS.league.season = 2023 AND TSS.league.name = 'Premier League' AND MPF.played >= 10
    ORDER BY
        TOP.overall_win_percentage DESC
)
SELECT * FROM WinsWithMostPlayedFormation;