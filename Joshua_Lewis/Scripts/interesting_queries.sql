--- Query 1 ---
SELECT address, COUNT(*) AS disaster_count
FROM disaster_events
GROUP BY address
ORDER BY disaster_count DESC
LIMIT 10;
---Querey 2 ---
SELECT 
  AVG(temperature_2m) AS avg_temp,
  AVG(wind_speed_10m) AS avg_wind,
  AVG(cloud_cover) AS avg_cloudiness,
  COUNT(*) AS event_count
FROM disaster_events;
---Query 3 ---
SELECT 
  type,
  COUNT(*) AS events,
  AVG(wind_speed_10m) AS avg_wind,
  MAX(wind_speed_10m) AS max_wind
FROM disaster_events
GROUP BY type
ORDER BY avg_wind DESC;
---Query 4 ---
SELECT 
  FORMAT_TIMESTAMP('%Y-%m', timestamp) AS month,
  COUNT(*) AS total_disasters
FROM disaster_events
GROUP BY month
ORDER BY month;
---Query 5 ---
SELECT *
FROM disaster_events
ORDER BY wind_gusts_10m DESC
LIMIT 5;

