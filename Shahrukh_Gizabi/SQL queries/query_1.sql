SELECT
    TSS.team.name AS team_name,
    TSS.fixtures.wins.total AS total_wins,
    TSS.fixtures.played.total AS matches_played,
    ROUND(TSS.goals.for.total.total / TSS.fixtures.played.total, 2) AS avg_goals_scored_per_game,
    ROUND(TSS.goals.against.total.total / TSS.fixtures.played.total, 2) AS avg_goals_conceded_per_game,
    TSS.clean_sheet.total AS total_clean_sheets
FROM
    soccer_premier_league_analytics.TeamSeasonStats AS TSS
WHERE
    TSS.league.season = 2023 AND TSS.league.name = 'Premier League' -- Assuming league name for specificity
ORDER BY
    total_wins DESC
LIMIT 5; -- Show top 5 teams