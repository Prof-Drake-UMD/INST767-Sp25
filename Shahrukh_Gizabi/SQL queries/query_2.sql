WITH TeamPerformance AS (
    SELECT
        team.name AS team_name,
        fixtures.wins.total AS total_wins,
        goals.for.minute.min_76_90.total AS goals_in_last_15_min,
        goals.for.total.total AS total_goals_for,
        NTILE(4) OVER (ORDER BY fixtures.wins.total DESC) AS win_quartile
    FROM
        soccer_premier_league_analytics.TeamSeasonStats
    WHERE
        league.season = 2023 AND league.name = 'Premier League' AND fixtures.played.total > 0 AND goals.for.total.total > 0
)
SELECT
    win_quartile,
    COUNT(team_name) AS number_of_teams_in_quartile,
    ROUND(AVG(SAFE_DIVIDE(goals_in_last_15_min, total_goals_for) * 100), 2) AS avg_perc_goals_in_last_15_min
FROM
    TeamPerformance
WHERE
    win_quartile = 1 OR win_quartile = 4 -- Top and Bottom quartiles
GROUP BY
    win_quartile
ORDER BY
    win_quartile;