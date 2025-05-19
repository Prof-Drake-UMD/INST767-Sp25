-- Analysis of which types of sports content perform best
WITH
event_categories AS (
  SELECT
    e.event_id,
    e.event_name,
    e.league_id,
    e.league_name,
    e.event_date,
    CASE
      WHEN e.home_score > e.away_score THEN 'Home Win'
      WHEN e.away_score > e.home_score THEN 'Away Win'
      ELSE 'Draw'
    END AS result_type,
    ABS(e.home_score - e.away_score) AS score_difference,
    CASE
      WHEN ABS(e.home_score - e.away_score) >= 3 THEN 'Blowout'
      WHEN ABS(e.home_score - e.away_score) <= 1 THEN 'Close Game'
      ELSE 'Standard Game'
    END AS game_type
  FROM
    sports_analytics.events e
  WHERE
    e.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
),

content_performance AS (
  SELECT
    v.video_id,
    v.title,
    v.content_type,
    v.published_at,
    ec.event_id,
    ec.event_name,
    ec.league_name,
    ec.result_type,
    ec.game_type,
    ec.event_date,
    DATE_DIFF(DATE(v.published_at), ec.event_date, DAY) AS days_after_event,
    vm.view_count,
    vm.like_count,
    vm.comment_count,
    vm.engagement_rate,
    -- Calculate per-day performance (views divided by days since publishing)
    vm.view_count / NULLIF(DATE_DIFF(vm.snapshot_date, DATE(v.published_at), DAY), 0) AS views_per_day
  FROM
    sports_analytics.youtube_videos v
  JOIN
    sports_analytics.video_metrics vm
  ON
    v.video_id = vm.video_id
  JOIN
    event_categories ec
  ON
    ARRAY_LENGTH(v.related_event_ids) > 0
    AND ec.event_id IN UNNEST(v.related_event_ids)
  WHERE
    DATE(v.published_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) AND CURRENT_DATE()
    AND DATE_DIFF(DATE(v.published_at), ec.event_date, DAY) BETWEEN 0 AND 14  -- Videos within 2 weeks of event
    AND vm.snapshot_date = (
      SELECT MAX(snapshot_date)
      FROM sports_analytics.video_metrics
      WHERE video_id = v.video_id
    )  -- Get most recent metrics
)

SELECT
  content_type,
  game_type,
  result_type,
  COUNT(DISTINCT video_id) AS num_videos,

  -- Performance metrics
  AVG(view_count) AS avg_views,
  AVG(like_count) AS avg_likes,
  AVG(comment_count) AS avg_comments,
  AVG(engagement_rate) AS avg_engagement_rate,
  AVG(views_per_day) AS avg_views_per_day,

  -- Day after event metrics
  AVG(IF(days_after_event <= 1, view_count, NULL)) AS avg_views_first_day,
  AVG(IF(days_after_event BETWEEN 2 AND 7, view_count, NULL)) AS avg_views_first_week,

  -- Popularity index
  AVG(engagement_rate) * LOG10(AVG(view_count) + 1) AS popularity_index,

  -- Longevity score (how well content performs over time)
  AVG(views_per_day) / AVG(IF(days_after_event <= 1, view_count / 1, NULL)) AS content_longevity
FROM
  content_performance
WHERE
  days_after_event >= 0  -- Ensure content was published after the event
GROUP BY
  content_type,
  game_type,
  result_type
HAVING
  COUNT(DISTINCT video_id) >= 5  -- Require at least 5 videos for significance
ORDER BY
  popularity_index DESC;