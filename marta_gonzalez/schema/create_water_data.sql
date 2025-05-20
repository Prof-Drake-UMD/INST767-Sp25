CREATE TABLE IF NOT EXISTS water_conditions (
  timestamp TIMESTAMP,
  site_id STRING,
  site_name STRING,
  streamflow_cfs FLOAT64,
  latitude FLOAT64,
  longitude FLOAT64
);