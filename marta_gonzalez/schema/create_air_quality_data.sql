CREATE TABLE IF NOT EXISTS air_quality_data (
  date DATE,
  borough STRING,
  pm25 FLOAT64,
  aqi INT64,
  o3 FLOAT64,
  no2 FLOAT64
);