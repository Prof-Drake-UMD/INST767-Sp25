-- Audience engagement patterns and comment sentiment analysis
WITH
event_comments AS (
  SELECT
    e.event_id,
    e.event_name,
    e.event_date,
    e.league_name,
    e.home_team_name,
    e.away_team_name,
    v.video_id,
    v.title AS video_title,
    c.comment_id,
    c.text AS comment_text,
    c.published_at AS comment_time,
    c.like_count AS comment_likes,
    c.sentiment_score,
    DATE_DIFF(DATE(c.published_at), e.event_date, DAY) AS days_from_event,
    DATE_DIFF(DATE(c.published_at), DATE(v.published_at), DAY) AS days_from_video_publish
  FROM
    sports_analytics.events e
  JOIN
    sports_analytics.youtube_videos v
  ON
    e.event_id IN UNNEST(v.related_event_ids)
  JOIN
    sports_analytics.video_comments c
  ON
    v.video_id = c.video_id
  WHERE
    e.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
    AND DATE_DIFF(DATE(c.published_at), e.event_date, DAY) BETWEEN 0 AND 14  -- Comments within 2 weeks
),

comment_analysis AS (
  SELECT
    event_id,
    event_name,
    video_id,
    video_title,
    days_from_event,

    -- Comment volume
    COUNT(comment_id) AS comment_count,

    -- Comment engagement
    AVG(comment_likes) AS avg_comment_likes,

    -- Sentiment metrics
    AVG(sentiment_score) AS avg_sentiment,
    STDDEV(sentiment_score) AS sentiment_std_dev,
    APPROX_QUANTILES(sentiment_score, 4)[OFFSET(1)] AS sentiment_25th_percentile,
    APPROX_QUANTILES(sentiment_score, 4)[OFFSET(3)] AS sentiment_75th_percentile,

    -- Sentiment distribution
    COUNTIF(sentiment_score > 0.5) AS positive_comments,
    COUNTIF(sentiment_score BETWEEN -0.5 AND 0.5) AS neutral_comments,
    COUNTIF(sentiment_score < -0.5) AS negative_comments
  FROM
    event_comments
  GROUP BY
    event_id,
    event_name,
    video_id,
    video_title,
    days_from_event
),

event_summary AS (
  SELECT
    event_id,
    event_name,

    -- Engagement metrics
    COUNT(DISTINCT video_id) AS total_videos,
    SUM(comment_count) AS total_comments,

    -- Sentiment metrics across all videos
    AVG(avg_sentiment) AS overall_avg_sentiment,

    -- Sentiment distribution
    SUM(positive_comments) AS total_positive_comments,
    SUM(neutral_comments) AS total_neutral_comments,
    SUM(negative_comments) AS total_negative_comments,

    -- Temporal patterns (comments by day after event)
    ARRAY_AGG(STRUCT(
      days_from_event,
      SUM(comment_count) AS day_comments,
      AVG(avg_sentiment) AS day_avg_sentiment
    ) ORDER BY days_from_event) AS daily_patterns,

    -- Engagement curve (how comments trend over time)
    SUM(IF(days_from_event <= 1, comment_count, 0)) / NULLIF(SUM(comment_count), 0) AS first_day_comment_ratio,

    -- Video-level comment stats
    ARRAY_AGG(STRUCT(
      video_id,
      video_title,
      comment_count,
      avg_sentiment,
      CONCAT(
        CAST(ROUND(positive_comments / NULLIF(comment_count, 0) * 100) AS STRING), '% pos, ',
        CAST(ROUND(negative_comments / NULLIF(comment_count
        CAST(ROUND(negative_comments / NULLIF(comment_count, 0) * 100) AS STRING), '% neg'
      ) AS sentiment_breakdown
    ) ORDER BY comment_count DESC LIMIT 10) AS top_commented_videos
  FROM
    comment_analysis
  GROUP BY
    event_id,
    event_name
)

SELECT
  es.event_id,
  es.event_name,
  e.event_date,
  e.league_name,
  CONCAT(e.home_team_name, ' vs. ', e.away_team_name) AS matchup,

  -- Engagement summary
  es.total_videos,
  es.total_comments,

  -- Sentiment analysis
  es.overall_avg_sentiment,

  -- Sentiment distribution metrics
  es.total_positive_comments,
  es.total_neutral_comments,
  es.total_negative_comments,

  -- Sentiment ratio
  ROUND(es.total_positive_comments / NULLIF(es.total_negative_comments, 0), 2) AS positive_to_negative_ratio,

  -- Sentiment distribution percentages
  ROUND(es.total_positive_comments / NULLIF(es.total_comments, 0) * 100, 1) AS positive_percentage,
  ROUND(es.total_neutral_comments / NULLIF(es.total_comments, 0) * 100, 1) AS neutral_percentage,
  ROUND(es.total_negative_comments / NULLIF(es.total_comments, 0) * 100, 1) AS negative_percentage,

  -- First day engagement metrics
  es.first_day_comment_ratio,

  -- Daily engagement patterns
  es.daily_patterns,

  -- Popular videos with sentiment
  es.top_commented_videos,

  -- Match outcome
  CASE
    WHEN e.home_score > e.away_score THEN CONCAT(e.home_team_name, ' win')
    WHEN e.away_score > e.home_score THEN CONCAT(e.away_team_name, ' win')
    ELSE 'Draw'
  END AS match_result,

  -- Emotion index (combination of comment volume and sentiment)
  LOG10(es.total_comments + 1) * (es.overall_avg_sentiment + 1) AS emotion_index
FROM
  event_summary es
JOIN
  sports_analytics.events e
ON
  es.event_id = e.event_id
WHERE
  es.total_comments >= 100  -- Ensure enough comments for meaningful analysis
ORDER BY
  es.total_comments DESC;