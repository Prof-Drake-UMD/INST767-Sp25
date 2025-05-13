-- Create video metrics table (daily snapshots)
CREATE OR REPLACE TABLE sports_analytics.video_metrics (
  video_id STRING NOT NULL,
  snapshot_date DATE NOT NULL,
  view_count INT64,
  like_count INT64,
  dislike_count INT64,
  favorite_count INT64,
  comment_count INT64,
  engagement_rate FLOAT64,  -- (likes + dislikes + comments) / views
  daily_views INT64,  -- Difference from previous day
  daily_likes INT64,  -- Difference from previous day
  daily_comments INT64,  -- Difference from previous day
  created_at TIMESTAMP
)
PARTITION BY snapshot_date
CLUSTER BY video_id;

