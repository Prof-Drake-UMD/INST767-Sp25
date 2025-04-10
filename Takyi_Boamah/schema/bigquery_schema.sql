-- BigQuery Data Schema for EcoFusion Pipeline

-- 1. Weather Snapshot Table
CREATE TABLE `ecofusion.weather_snapshot` (
  state STRING,
  city STRING,
  datetime TIMESTAMP,
  temp_celsius FLOAT64,
  feels_like_celsius FLOAT64,
  humidity_percent INT64,
  pressure_hpa INT64,
  weather_main STRING,
  weather_description STRING,
  wind_speed_mps FLOAT64,
  wind_deg INT64,
  cloud_percent INT64,
  visibility_m INT64
);

-- 2. Carbon Emissions Estimates Table (Carbon Interface API)
CREATE TABLE `ecofusion.carbon_emissions` (
  state STRING,
  country STRING,
  estimated_at TIMESTAMP,
  electricity_value_mwh FLOAT64,
  carbon_g INT64,
  carbon_lb FLOAT64,
  carbon_kg FLOAT64,
  carbon_mt FLOAT64
);

-- 3. Electricity Emissions by Fuel Type (EIA)
CREATE TABLE `ecofusion.state_electricity_emissions` (
  state STRING,
  year INT64,
  fuel_type STRING,
  fuel_description STRING,
  co2_thousand_mt FLOAT64
);

-- View for easier state-level aggregation
CREATE VIEW `ecofusion.state_emission_summary` AS
SELECT
  state,
  year,
  SUM(CASE WHEN LOWER(fuel_type) = 'coal' THEN co2_thousand_mt ELSE 0 END) AS coal_emissions,
  SUM(CASE WHEN LOWER(fuel_type) = 'ng' THEN co2_thousand_mt ELSE 0 END) AS gas_emissions,
  SUM(CASE WHEN LOWER(fuel_type) = 'pet' THEN co2_thousand_mt ELSE 0 END) AS petroleum_emissions,
  SUM(CASE WHEN LOWER(fuel_type) = 'oth' THEN co2_thousand_mt ELSE 0 END) AS other_emissions,
  SUM(co2_thousand_mt) AS total_emissions
FROM `ecofusion.state_electricity_emissions`
GROUP BY state, year;
