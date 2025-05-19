-- Create teams table
CREATE OR REPLACE TABLE sports_analytics.teams (
  team_id STRING NOT NULL,
  team_name STRING,
  league_id STRING,
  league_name STRING,
  country STRING,
  stadium STRING,
  website STRING,
  facebook STRING,
  twitter STRING,
  instagram STRING,
  description STRING,
  logo_url STRING,
  jersey_url STRING,
  founded INT64,
  team_metadata JSON,  -- Additional team details as JSON
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

