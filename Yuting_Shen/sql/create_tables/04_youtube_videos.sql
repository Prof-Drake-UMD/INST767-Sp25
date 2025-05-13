-- Create YouTube videos table
CREATE OR REPLACE TABLE sports_analytics.youtube_videos (
  video_id STRING NOT NULL,
  channel_id STRING,
  channel_title STRING,
  title STRING,
  description STRING,
  published_at TIMESTAMP,
  tags ARRAY<STRING>,
  duration INT64,  -- Duration in seconds
  category_id STRING,
  default_language STRING,
  content_type STRING,  -- Highlight, full game, analysis, etc.
  sports_related_tags ARRAY<STRING>,
  related_event_ids ARRAY<STRING>,
  related_team_ids ARRAY<STRING>,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY DATE(published_at)
CLUSTER BY channel_id;