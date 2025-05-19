/*1.Correlation Between GDP and Air Quality Across Cities*/
SELECT
  country,
  gdp_value,
  aqi,
  ROUND(SAFE_DIVIDE(gdp_value, aqi), 2) AS gdp_per_aqi_ratio
FROM `climate-data-pipeline-457720.climate_data.combined_view`
WHERE gdp_value IS NOT NULL AND aqi IS NOT NULL
ORDER BY gdp_per_aqi_ratio DESC;



/*2.Impact of Humidity on Air Quality*/
SELECT
  high_humidity_flag,
  COUNT(*) AS cities,
  ROUND(AVG(aqi), 2) AS avg_aqi,
  ROUND(STDDEV(aqi), 2) AS std_dev_aqi
FROM `climate-data-pipeline-457720.climate_data.combined_view`
WHERE aqi IS NOT NULL
GROUP BY high_humidity_flag;



/*3.GDP Growth vs. Air Quality (Detecting Green Growth Patterns)*/
SELECT
  country,
  gdp_growth_rate,
  aqi,
  CASE
    WHEN gdp_growth_rate > 0 AND aqi <= 50 THEN 'Sustainable Growth'
    WHEN gdp_growth_rate > 0 AND aqi > 50 THEN 'Unsustainable Growth'
    WHEN gdp_growth_rate <= 0 THEN 'Shrinking Economy'
    ELSE 'Unknown'
  END AS classification
FROM `climate-data-pipeline-457720.climate_data.combined_view`
WHERE gdp_growth_rate IS NOT NULL AND aqi IS NOT NULL
ORDER BY gdp_growth_rate DESC;




/*4.Relationship Between Temperature Categories and GDP Levels*/
SELECT
  temperature_category,
  COUNT(*) AS num_cities,
  ROUND(AVG(gdp_value), 2) AS avg_gdp,
  ROUND(MIN(gdp_value), 2) AS min_gdp,
  ROUND(MAX(gdp_value), 2) AS max_gdp
FROM `climate-data-pipeline-457720.climate_data.combined_view`
WHERE gdp_value IS NOT NULL
GROUP BY temperature_category
ORDER BY avg_gdp DESC;



/*5.Top cities with best economic growth & clean air*/
SELECT
  city,
  country,
  gdp_growth_rate,
  aqi,
  temperature_category
FROM `climate-data-pipeline-457720.climate_data.combined_view`
WHERE aqi <= 50 AND gdp_growth_rate > 0
ORDER BY gdp_growth_rate DESC
LIMIT 10;


