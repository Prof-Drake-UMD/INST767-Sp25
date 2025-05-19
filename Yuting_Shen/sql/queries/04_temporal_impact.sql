-- Temporal analysis of how long event impact lasts
WITH
event_daily_metrics AS (
  SELECT
    e.event_id,
    e.event_name,
    e.event_date,
    e.league_name,
    e.home_team_name,
    e.away_team_name,
    st.date AS interest_date,
    DATE_DIFF(st.date, e.event_date, DAY) AS days_from_event,

    -- Average search interest
    AVG(st.interest_score) AS avg_search_interest,

    -- Video metrics
    (
      SELECT COUNT(DISTINCT v.video_id)
      FROM sports_analytics.youtube_videos v
      WHERE
        e.event_id IN UNNEST(v.related_event_ids)
        AND DATE(v.published_at) = st.date
    ) AS new_videos_count,

    -- Daily views for event-related videos
    (
      SELECT SUM(vm.daily_views)
      FROM sports_analytics.youtube_videos v
      JOIN sports_analytics.video_metrics vm
        ON v.video_id = vm.video_id
      WHERE
        e.event_id IN UNNEST(v.related_event_ids)
        AND vm.snapshot_date = st.date
    ) AS daily_video_views,

    -- Daily engagement metrics
    (
      SELECT SUM(vm.daily_likes)
      FROM sports_analytics.youtube_videos v
      JOIN sports_analytics.video_metrics vm
        ON v.video_id = vm.video_id
      WHERE
        e.event_id IN UNNEST(v.related_event_ids)
        AND vm.snapshot_date = st.date
    ) AS daily_video_likes
  FROM
    sports_analytics.events e
  JOIN
    sports_analytics.search_trends st
  ON
    st.related_event_id = e.event_id
    OR st.related_team_id IN (e.home_team_id, e.away_team_id)
  WHERE
    -- Look at up to 30 days after events
    DATE_DIFF(st.date, e.event_date, DAY) BETWEEN 0 AND 30
  GROUP BY
    e.event_id, e.event_name, e.event_date, e.league_name,
    e.home_team_name, e.away_team_name, st.date
)

SELECT
  event_id,
  event_name,
  event_date,
  league_name,
  CONCAT(home_team_name, ' vs. ', away_team_name) AS matchup,

  -- Baseline metrics (day of event)
  MAX(IF(days_from_event = 0, avg_search_interest, NULL)) AS event_day_interest,
  MAX(IF(days_from_event = 0, daily_video_views, NULL)) AS event_day_views,

  -- Half-life calculation (interest)
  MIN(IF(avg_search_interest <= MAX(IF(days_from_event = 0, avg_search_interest, NULL)) / 2
         AND days_from_event > 0,
         days_from_event, NULL)) AS interest_half_life_days,

  -- Half-life calculation (views)
  MIN(IF(daily_video_views <= MAX(IF(days_from_event = 0, daily_video_views, NULL)) / 2
         AND days_from_event > 0,
         days_from_event, NULL)) AS views_half_life_days,

  -- Post-event metrics snapshots
  MAX(IF(days_from_event = 1, avg_search_interest, NULL)) AS day_1_interest,
  MAX(IF(days_from_event = 3, avg_search_interest, NULL)) AS day_3_interest,
  MAX(IF(days_from_event = 7, avg_search_interest, NULL)) AS day_7_interest,
  MAX(IF(days_from_event = 14, avg_search_interest, NULL)) AS day_14_interest,
  MAX(IF(days_from_event = 30, avg_search_interest, NULL)) AS day_30_interest,

  -- Decay rates (percent of interest/views retained after 7 days)
  MAX(IF(days_from_event = 7, avg_search_interest, NULL)) /
    NULLIF(MAX(IF(days_from_event = 0, avg_search_interest, NULL)), 0) AS interest_retention_7d,

  MAX(IF(days_from_event = 7, daily_video_views, NULL)) /
    NULLIF(MAX(IF(days_from_event = 0, daily_video_views, NULL)), 0) AS views_retention_7d,

  -- Video production intensity (total new videos in first week)
  SUM(IF(days_from_event BETWEEN 0 AND 7, new_videos_count, 0)) AS first_week_video_count,

  -- Aggregate daily metrics into arrays for time series analysis
  ARRAY_AGG(STRUCT(
    days_from_event,
    avg_search_interest,
    daily_video_views,
    daily_video_likes,
    new_videos_count
  ) ORDER BY days_from_event) AS daily_metrics
FROM
  event_daily_metrics
GROUP BY
  event_id,
  event_name,
  event_date,
  league_name,
  home_team_name,
  away_team_name
HAVING
  MAX(IF(days_from_event = 0, avg_search_interest, NULL)) IS NOT NULL
  AND MAX(IF(days_from_event = 0, daily_video_views, NULL)) IS NOT NULL
ORDER BY
  event_date DESC;