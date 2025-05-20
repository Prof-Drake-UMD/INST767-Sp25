CREATE TABLE IF NOT EXISTS weather_data (
  timestamp TIMESTAMP,
  latitude FLOAT64,
  longitude FLOAT64,
  temperature_c FLOAT64,
  precipitation_mm FLOAT64,
  wind_speed_mps FLOAT64
);