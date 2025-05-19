## Traffic Table

CREATE TABLE dataset.Traffic (
  date DATE,
  vol FLOAT64
);

CREATE TABLE dataset.Metro (
  date DATE,
  ridership FLOAT64
);


CREATE TABLE dataset.Weather (
  date DATE,
  mean_temp FLOAT64, 
  max_temp FLOAT64,
  min_temp FLOAT64,
  weather_code INT64,
  precipitation_sum FLOAT64
);

