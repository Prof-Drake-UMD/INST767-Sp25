-- Create sports events table
CREATE OR REPLACE TABLE sports_analytics.events (
  event_id STRING NOT NULL,
  event_name STRING,
  event_date DATE,
  event_time TIME,
  event_timestamp TIMESTAMP,
  league_id STRING,
  league_name STRING,
  home_team_id STRING,
  home_team_name STRING,
  away_team_id STRING,
  away_team_name STRING,
  home_score INT64,
  away_score INT64,
  status STRING,
  season STRING,
  round STRING,
  venue STRING,
  country STRING,
  event_metadata JSON,  -- Additional event details as JSON
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY league_id, home_team_id, away_team_id;