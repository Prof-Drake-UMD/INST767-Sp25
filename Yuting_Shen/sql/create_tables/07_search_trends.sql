-- Create search trends table
CREATE OR REPLACE TABLE sports_analytics.search_trends (
  keyword STRING NOT NULL,
  date DATE NOT NULL,
  interest_score INT64,  -- 0-100 scale
  related_event_id STRING,
  related_team_id STRING,
  created_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY keyword;