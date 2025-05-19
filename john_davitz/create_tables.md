## Traffic Table

CREATE TABLE dataset.Traffic (
  id STRING,
  date DATE,
  segment STRING,
  reading_date DATE,
  daily_sum FLOAT64
);

CREATE TABLE dataset.Metro (
  id STRING,
  date DATE,
  ridership FLOAT64
);


CREATE TABLE dataset.Weather (
  id STRING,
  date DATE,
  mean_temp FLOAT64, 
  max_temp FLOAT64,
  min_temp FLOAT64,
  weather_code INT64,
  precipitation_sum FLOAT64
);

