CREATE TABLE disaster_events (
  event_id STRING,
  type STRING,
  title STRING,
  timestamp DATETIME,
  lat FLOAT64,
  lon FLOAT64,
  address STRING,
  temperature_2m FLOAT64,
  apparent_temperature FLOAT64,
  precipitation FLOAT64,
  wind_speed_10m FLOAT64,
  wind_gusts_10m FLOAT64,
  cloud_cover FLOAT64,
  weather_code INT64
);
