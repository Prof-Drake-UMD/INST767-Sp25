
-- Query 1. Monthly firearm incident trend per city (2020–2024)(if covid had an impact on city-level firearm incidents)
SELECT
  city,
  FORMAT_DATE('%Y-%m', incident_date) AS month,
  COUNT(*) AS incident_count
FROM `gv-etl-spring.gun_violence_dataset.firearm_incidents`
WHERE 
  city IS NOT NULL
  AND incident_date BETWEEN '2020-01-01' AND '2024-12-31'
GROUP BY city, month
ORDER BY city, month;

-- Query 2: Top 5 neighborhoods/boroughs per city and states with the most firearm-related incidents 
SELECT
  city,
  location,
  total_incidents
FROM (
  SELECT
    city,
    location,
    COUNT(*) AS total_incidents,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY COUNT(*) DESC) AS rn
  FROM `gv-etl-spring.gun_violence_dataset.firearm_incidents`
  WHERE city IS NOT NULL AND location IS NOT NULL
  GROUP BY city, location
)
WHERE rn <= 5;

-- Query 3: Weekday vs. weekend trends for firearm incidents
SELECT
  city,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM incident_date) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  COUNT(*) AS total
FROM `gv-etl-spring.gun_violence_dataset.firearm_incidents`
WHERE city IS NOT NULL
GROUP BY city, day_type
ORDER BY city, day_type;

-- Query 4: National firearm death type distribution from CDC
SELECT
  description,
  COUNT(*) AS case_count
FROM `gv-etl-spring.gun_violence_dataset.firearm_incidents`
WHERE source_dataset = 'CDC State-Level'
GROUP BY description
ORDER BY case_count DESC
LIMIT 10;

-- Query 5: Correlation of urban firearm incidents and national CDC firearm deaths by year
WITH city_data AS (
  SELECT
    EXTRACT(YEAR FROM incident_date) AS year,
    COUNT(*) AS city_total
  FROM `gv-etl-spring.gun_violence_dataset.firearm_incidents`
  WHERE city IN ('New York City', 'Washington DC')
  GROUP BY year
),
cdc_data AS (
  SELECT
    EXTRACT(YEAR FROM incident_date) AS year,
    COUNT(*) AS national_total
  FROM `gv-etl-spring.gun_violence_dataset.firearm_incidents`
  WHERE source_dataset = 'CDC State-Level'
  GROUP BY year
)
SELECT
  cdc_data.year,
  city_data.city_total,
  cdc_data.national_total,
  ROUND(city_data.city_total / cdc_data.national_total, 3) AS contribution_ratio
FROM cdc_data
JOIN city_data ON cdc_data.year = city_data.year
ORDER BY cdc_data.year;

