-- Regional engagement analysis with video performance
WITH
team_regional_interest AS (
  SELECT
    t.team_id,
    t.team_name,
    t.country AS team_country,
    ri.region,
    ri.date,
    AVG(ri.interest_score) AS avg_interest_score
  FROM
    sports_analytics.teams t
  JOIN
    sports_analytics.regional_interest ri
  ON
    ri.related_team_id = t.team_id
  WHERE
    ri.date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
  GROUP BY
    t.team_id,
    t.team_name,
    t.country,
    ri.region,
    ri.date
),

team_video_performance AS (
  SELECT
    t.team_id,
    t.team_name,
    v.video_id,
    v.title,
    DATE(v.published_at) AS publish_date,
    vm.snapshot_date,
    vm.view_count,
    vm.like_count,
    vm.comment_count
  FROM
    sports_analytics.teams t
  JOIN
    sports_analytics.youtube_videos v
  ON
    ARRAY_LENGTH(v.related_team_ids) > 0
    AND t.team_id IN UNNEST(v.related_team_ids)
  JOIN
    sports_analytics.video_metrics vm
  ON
    v.video_id = vm.video_id
  WHERE
    DATE(v.published_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
)

SELECT
  tri.team_name,
  tri.team_country,
  tri.region,

  -- Regional interest metrics
  AVG(tri.avg_interest_score) AS avg_regional_interest,

  -- Video performance in region (estimated)
  COUNT(DISTINCT tvp.video_id) AS total_videos,
  SUM(tvp.view_count) / COUNT(DISTINCT tvp.video_id) AS avg_views_per_video,
  SUM(tvp.like_count) / COUNT(DISTINCT tvp.video_id) AS avg_likes_per_video,

  -- Engagement rate calculation
  SUM(tvp.like_count + tvp.comment_count) / NULLIF(SUM(tvp.view_count), 0) * 100 AS engagement_rate,

  -- Affinity score (interest vs. team's home country)
  CASE
    WHEN tri.region = tri.team_country THEN 'Home'
    ELSE 'Away'
  END AS market_type,

  -- Calculate affinity factor (how interest compares to team's home market)
  AVG(tri.avg_interest_score) /
    MAX(IF(tri.region = tri.team_country, tri.avg_interest_score, NULL))
      OVER(PARTITION BY tri.team_id) AS affinity_factor
FROM
  team_regional_interest tri
JOIN
  team_video_performance tvp
ON
  tri.team_id = tvp.team_id
  AND tri.date = tvp.snapshot_date
GROUP BY
  tri.team_name,
  tri.team_country,
  tri.region,
  tri.team_id
HAVING
  COUNT(DISTINCT tvp.video_id) >= 5  -- Require at least 5 videos for significance
ORDER BY
  avg_regional_interest DESC,
  engagement_rate DESC;