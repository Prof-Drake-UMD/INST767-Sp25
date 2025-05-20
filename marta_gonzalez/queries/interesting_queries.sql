/*Correlation Between Precipitation and Streamflow (lagged)*/
SELECT
  w.timestamp,
  w.precipitation_mm,
  wd.streamflow_cfs,
  LAG(wd.streamflow_cfs, 1) OVER (ORDER BY w.timestamp) AS previous_streamflow
FROM
  `your_dataset.weather_data` w
JOIN
  `your_dataset.water_data` wd
ON
  w.timestamp = wd.timestamp
WHERE
  w.precipitation_mm IS NOT NULL AND wd.streamflow_cfs IS NOT NULL
ORDER BY
  w.timestamp;



/*When is Air Quality the Worst (hour of day)?*/
SELECT
  EXTRACT(HOUR FROM timestamp) AS hour,
  AVG(pm2_5) AS avg_pm2_5,
  AVG(ozone) AS avg_ozone
FROM
  `your_dataset.air_quality_data`
WHERE
  pm2_5 IS NOT NULL
GROUP BY
  hour
ORDER BY
  avg_pm2_5 DESC
LIMIT 5;



/*Top 5 Days with Worst Air Quality and Associated Weather*/
SELECT
  DATE(a.timestamp) AS date,
  AVG(a.pm2_5) AS avg_pm2_5,
  AVG(w.temperature_2m) AS avg_temp,
  AVG(w.wind_speed_10m) AS avg_wind
FROM
  `your_dataset.air_quality_data` a
JOIN
  `your_dataset.weather_data` w
ON
  a.timestamp = w.timestamp
GROUP BY
  date
ORDER BY
  avg_pm2_5 DESC
LIMIT 5;



/*Percent Change in Streamflow after Rain Events*/
WITH flow_diff AS (
  SELECT
    timestamp,
    streamflow_cfs,
    LAG(streamflow_cfs) OVER (ORDER BY timestamp) AS previous_flow
  FROM `your_dataset.water_data`
)
SELECT
  timestamp,
  ((streamflow_cfs - previous_flow) / previous_flow) * 100 AS percent_change
FROM flow_diff
WHERE previous_flow IS NOT NULL
ORDER BY percent_change DESC
LIMIT 10;



/*Multivariate Environmental Condition Snapshot*/
SELECT
  w.timestamp,
  w.temperature_2m,
  w.precipitation_mm,
  a.pm2_5,
  a.ozone,
  wd.streamflow_cfs
FROM
  `your_dataset.weather_data` w
JOIN
  `your_dataset.air_quality_data` a
ON
  w.timestamp = a.timestamp
JOIN
  `your_dataset.water_data` wd
ON
  w.timestamp = wd.timestamp
WHERE
  w.precipitation_mm > 2
  AND a.pm2_5 > 25
ORDER BY
  w.timestamp;
