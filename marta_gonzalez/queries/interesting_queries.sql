/*How does rainfall impact air quality (PM2.5) in different boroughs?*/
SELECT
  aq.borough,
  DATE(w.timestamp) AS date,
  AVG(w.precipitation_mm) AS avg_precipitation,
  AVG(aq.pm25) AS avg_pm25
FROM
  air_quality_data aq
JOIN
  weather_data w
ON
  DATE(w.timestamp) = aq.date
GROUP BY
  borough, date
ORDER BY
  date, borough;


/*Identify days with highest wind speed and their impact on AQI*/
SELECT
  w.timestamp,
  w.wind_speed_mps,
  aq.borough,
  aq.aqi
FROM
  weather_data w
JOIN
  air_quality_data aq
ON
  DATE(w.timestamp) = aq.date
WHERE
  w.wind_speed_mps > 10
ORDER BY
  w.wind_speed_mps DESC
LIMIT 50;


/*Compare average streamflow levels before and after rainy days*/
WITH rain_days AS (
  SELECT DATE(timestamp) AS rain_date
  FROM weather_data
  WHERE precipitation_mm > 5.0
),
streamflow_lagged AS (
  SELECT
    wc.timestamp,
    DATE(wc.timestamp) AS date,
    wc.streamflow_cfs,
    LAG(wc.streamflow_cfs) OVER (PARTITION BY site_id ORDER BY wc.timestamp) AS prev_streamflow,
    wc.site_id
  FROM water_conditions wc
)
SELECT
  site_id,
  date,
  streamflow_cfs,
  prev_streamflow,
  streamflow_cfs - prev_streamflow AS change
FROM
  streamflow_lagged
WHERE
  date IN (SELECT rain_date FROM rain_days)
ORDER BY
  change DESC;


/*Find boroughs with consistently high AQI and no rain over a 7-day period*/
SELECT
  borough,
  COUNT(*) AS high_aqi_days
FROM
  air_quality_data aq
JOIN
  weather_data w
ON
  DATE(w.timestamp) = aq.date
WHERE
  aq.aqi > 100 AND w.precipitation_mm < 1
GROUP BY
  borough
HAVING
  high_aqi_days >= 7
ORDER BY
  high_aqi_days DESC;


/*Window function: 7-day rolling average temperature and AQI*/
SELECT
  borough,
  date,
  AVG(pm25) OVER (PARTITION BY borough ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_pm25,
  AVG(temperature_c) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_temp
FROM
  air_quality_data aq
JOIN
  weather_data w
ON
  aq.date = DATE(w.timestamp)
WHERE
  borough = 'Manhattan'
ORDER BY
  date;
