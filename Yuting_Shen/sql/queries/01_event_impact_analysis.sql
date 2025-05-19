-- Analysis of how sporting events affect online engagement
WITH
event_search_interest AS (
  SELECT
    e.event_id,
    e.event_name,
    e.event_date,
    e.home_team_name,
    e.away_team_name,
    e.home_score,
    e.away_score,
    s.date AS search_date,
    s.keyword,
    s.interest_score,
    -- Calculate days relative to event
    DATE_DIFF(s.date, e.event_date, DAY) AS days_from_event
  FROM
    sports_analytics.events e
  JOIN
    sports_analytics.search_trends s
  ON
    (s.related_team_id = e.home_team_id OR s.related_team_id = e.away_team_id)
    OR s.related_event_id = e.event_id
  WHERE
    -- Look at 7 days before and after events
    DATE_DIFF(s.date, e.event_date, DAY) BETWEEN -7 AND 7
),

event_video_metrics AS (
  SELECT
    e.event_id,
    e.event_date,
    v.video_id,
    v.title,
    DATE(v.published_at) AS publish_date,
    DATE_DIFF(DATE(v.published_at), e.event_date, DAY) AS days_from_event,
    vm.view_count,
    vm.like_count,
    vm.comment_count,
    vm.engagement_rate
  FROM
    sports_analytics.events e
  JOIN
    sports_analytics.youtube_videos v
  ON
    ARRAY_LENGTH(v.related_event_ids) > 0
    AND e.event_id IN UNNEST(v.related_event_ids)
  JOIN
    sports_analytics.video_metrics vm
  ON
    v.video_id = vm.video_id
  WHERE
    DATE_DIFF(DATE(v.published_at), e.event_date, DAY) BETWEEN 0 AND 7
    -- Focus on videos published on the event day or up to 7 days after
)

SELECT
  e.event_id,
  e.event_name,
  e.event_date,
  e.home_team_name,
  e.away_team_name,
  CONCAT(e.home_score, '-', e.away_score) AS result,
  CASE
    WHEN e.home_score > e.away_score THEN e.home_team_name
    WHEN e.away_score > e.home_score THEN e.away_team_name
    ELSE 'Draw'
  END AS winner,

  -- Search interest metrics
  ARRAY_AGG(DISTINCT STRUCT(
    si.days_from_event,
    si.keyword,
    si.interest_score
  ) ORDER BY si.days_from_event) AS daily_search_interest,

  -- Calculate pre-event, event day, and post-event search interest
  AVG(IF(si.days_from_event < 0, si.interest_score, NULL)) AS avg_pre_event_interest,
  AVG(IF(si.days_from_event = 0, si.interest_score, NULL)) AS event_day_interest,
  AVG(IF(si.days_from_event > 0, si.interest_score, NULL)) AS avg_post_event_interest,

  -- Calculate peak interest
  MAX(si.interest_score) AS peak_interest_score,

  -- Video engagement metrics
  COUNT(DISTINCT vm.video_id) AS video_count,
  SUM(vm.view_count) AS total_views,
  SUM(vm.like_count) AS total_likes,
  SUM(vm.comment_count) AS total_comments,

  -- Engagement metrics by time period
  AVG(vm.engagement_rate) AS avg_engagement_rate,

  -- Calculate impact score (normalized combination of metrics)
  (
    MAX(si.interest_score) / 100 * 0.5 +
    LOG10(SUM(vm.view_count) + 1) / 8 * 0.3 +
    AVG(vm.engagement_rate) * 0.2
  ) AS event_impact_score
FROM
  sports_analytics.events e
LEFT JOIN
  event_search_interest si
ON
  e.event_id = si.event_id
LEFT JOIN
  event_video_metrics vm
ON
  e.event_id = vm.event_id
WHERE
  e.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
GROUP BY
  e.event_id,
  e.event_name,
  e.event_date,
  e.home_team_name,
  e.away_team_name,
  e.home_score,
  e.away_score
ORDER BY
  event_impact_score DESC
LIMIT 50;

