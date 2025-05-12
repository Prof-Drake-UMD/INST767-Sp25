WITH TeamCategories AS (
    SELECT
        team.name AS team_name,
        league.season,
        (CASE WHEN goals.for.total.total >= (
                SELECT PERCENTILE_CONT(g.for.total.total, 0.75) OVER()
                FROM soccer_premier_league_analytics.TeamSeasonStats WHERE league.season = 2023 AND league.name = 'Premier League' LIMIT 1
            ) THEN 'Strong Attacking' ELSE 'Other'
        END) AS attack_category,
        (CASE WHEN goals.against.total.total <= (
                SELECT PERCENTILE_CONT(g.against.total.total, 0.25) OVER()
                FROM soccer_premier_league_analytics.TeamSeasonStats WHERE league.season = 2023 AND league.name = 'Premier League' LIMIT 1
            ) THEN 'Strong Defensive' ELSE 'Other'
        END) AS defense_category
    FROM
        soccer_premier_league_analytics.TeamSeasonStats
    WHERE league.season = 2023 AND league.name = 'Premier League'
)
SELECT
    COUNT(M.match_id) AS num_matches_considered,
    AVG(M.score.fullTime.home) AS avg_home_goals_strong_attack_vs_strong_defense,
    AVG(M.score.fullTime.away) AS avg_away_goals_strong_attack_vs_strong_defense,
    AVG(M.score.fullTime.home + M.score.fullTime.away) AS avg_total_goals_in_match
FROM
    soccer_premier_league_analytics.Matches AS M
JOIN
    TeamCategories AS H_Cat ON M.homeTeam.name = H_Cat.team_name AND M.season.startDate BETWEEN '2023-08-01' AND '2024-07-31' -- Approximate season match
JOIN
    TeamCategories AS A_Cat ON M.awayTeam.name = A_Cat.team_name AND M.season.startDate BETWEEN '2023-08-01' AND '2024-07-31' -- Approximate season match
WHERE
    M.competition.name = 'Premier League'
    AND M.status = 'FINISHED'
    AND H_Cat.attack_category = 'Strong Attacking'
    AND A_Cat.defense_category = 'Strong Defensive';