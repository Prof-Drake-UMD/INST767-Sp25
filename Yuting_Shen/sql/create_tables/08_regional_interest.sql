-- Create regional search interest table
CREATE OR REPLACE TABLE sports_analytics.regional_interest (
  keyword STRING NOT NULL,
  region STRING NOT NULL,  -- Country or region code
  date DATE NOT NULL,
  interest_score INT64,  -- 0-100 scale
  related_event_id STRING,
  related_team_id STRING,
  created_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY keyword, region;