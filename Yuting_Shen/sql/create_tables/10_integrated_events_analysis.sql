-- Create integrated events analysis table (denormalized for analysis)
CREATE OR REPLACE TABLE sports_analytics.integrated_events_analysis (
  event_id STRING NOT NULL,
  event_date DATE NOT NULL,
  event_name STRING,
  league_id STRING,
  league_name STRING,
  home_team_id STRING,
  home_team_name STRING,
  away_team_id STRING,
  away_team_name STRING,
  home_score INT64,
  away_score INT64,

  -- Video metrics
  related_videos ARRAY
    STRUCT
      video_id STRING,
      title STRING,
      published_at TIMESTAMP,
      view_count INT64,
      like_count INT64,
      comment_count INT64
    >
  >,
  total_video_views INT64,
  total_video_likes INT64,
  total_video_comments INT64,
  video_count INT64,

  -- Search interest metrics
  search_metrics ARRAY
    STRUCT
      keyword STRING,
      date DATE,
      interest_score INT64
    >
  >,
  avg_search_interest_pre_event FLOAT64,  -- Average for 7 days before
  avg_search_interest_post_event FLOAT64,  -- Average for 7 days after
  peak_search_interest INT64,
  peak_search_date DATE,

  -- Integrated metrics
  engagement_score FLOAT64,  -- Composite score of video and search metrics
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY league_id, home_team_id, away_team_id;