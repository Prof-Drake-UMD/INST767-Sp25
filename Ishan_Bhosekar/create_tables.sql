/*  Creating the Weather Table  */
CREATE TABLE `climate-data-pipeline-457720.climate_data.weather` (
  city STRING,
  timestamp TIMESTAMP,
  temperature FLOAT64,
  humidity INT64,
  wind_speed FLOAT64,
  weather_description STRING,
  temperature_category STRING,
  high_humidity_flag INT64
);

/* Creating the Air_Quality Table */
CREATE TABLE `climate-data-pipeline-457720.climate_data.air_quality` (
  city STRING,
  timestamp TIMESTAMP,
  aqi INT64,
  pm25 FLOAT64,
  pm10 FLOAT64,
  no2 FLOAT64,
  so2 FLOAT64,
  co FLOAT64,
  o3 FLOAT64,
  aqi_category STRING
);

/*  Creating the GDP Table  */
CREATE TABLE `climate-data-pipeline-457720.climate_data.gdp` (
  country STRING,
  year INT64,
  value FLOAT64,
  indicator STRING,
  gdp_growth_rate FLOAT64
);

/*  Creating the Country Code Mapping Table  */

CREATE TABLE `climate-data-pipeline-457720.climate_data.city_country_mapping` (
  city STRING,
  country_code STRING
);


