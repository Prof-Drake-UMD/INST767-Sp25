CREATE OR REPLACE VIEW `climate-data-pipeline-457720.climate_data.combined_view` AS
SELECT
  w.city,
  cc.country_code AS country,
  w.timestamp AS weather_time,
  w.temperature,
  w.temperature_category,
  w.humidity,
  w.high_humidity_flag,
  a.timestamp AS air_time,
  a.aqi,
  a.aqi_category,
  g.year,
  g.gdp_value,
  g.gdp_growth_rate
FROM `climate-data-pipeline-457720.climate_data.weather` w
LEFT JOIN `climate-data-pipeline-457720.climate_data.city_country_mapping` cc
  ON w.city = cc.city
LEFT JOIN `climate-data-pipeline-457720.climate_data.air_quality` a
  ON w.city = a.city
LEFT JOIN (
  SELECT country, year, value AS gdp_value, gdp_growth_rate
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY country ORDER BY year DESC) AS rn
    FROM `climate-data-pipeline-457720.climate_data.gdp`
    WHERE value IS NOT NULL
  )
  WHERE rn = 1
) g
ON cc.country_code = g.country;
