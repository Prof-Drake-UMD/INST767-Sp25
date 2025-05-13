-- Create video comments table
CREATE OR REPLACE TABLE sports_analytics.video_comments (
  comment_id STRING NOT NULL,
  video_id STRING NOT NULL,
  author_name STRING,
  text STRING,
  published_at TIMESTAMP,
  like_count INT64,
  sentiment_score FLOAT64,  -- Preprocessed sentiment (-1 to 1)
  parent_id STRING,  -- For reply comments
  created_at TIMESTAMP
)
PARTITION BY DATE(published_at)
CLUSTER BY video_id;