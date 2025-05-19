# 1 Days with the fewest riders
````
WITH metro_with_threshold AS (
  SELECT 
    date,
    ridership,
    PERCENTILE_CONT(ridership, 0.1) OVER() AS threshold
  FROM instfinal-459621.inst_final.metro
)
SELECT 
  M.date,
  M.ridership,
  W.mean_temp,
  W.precipitation_sum,
  W.weather_code
FROM metro_with_threshold M
JOIN inst_final.weather W ON M.date = W.date
WHERE M.ridership <= M.threshold
ORDER BY M.ridership;
````

# 2 Average weather conditions for the most rides
````
SELECT
  W.weather_code,
  ROUND(AVG(W.mean_temp), 1) AS avg_temp,
  ROUND(AVG(W.precipitation_sum), 2) AS avg_precip,
  ROUND(AVG(M.ridership), 0) AS avg_ridership,
  COUNT(*) AS day_count
FROM inst_final.metro M
JOIN inst_final.weather W ON M.date = W.date
GROUP BY W.weather_code
ORDER BY avg_ridership DESC;
````

# 3 Ridership on weekday vs weekend. 
```
SELECT
  FORMAT_DATE('%m', date) AS month,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  SUM(ridership) AS total_ridership
FROM inst_final.metro
GROUP BY month, day_type
ORDER BY month, day_type;

```
# 4 Transit methods on Sunny vs non sunny days

```
WITH joined_data AS (
  SELECT
    w.date,
    CASE WHEN w.weather_code = 0 THEN 'Sunny' ELSE 'Non-Sunny' END AS weather_type,
    m.ridership,
    t.vol AS traffic_volume
  FROM inst_final.weather w
  JOIN inst_final.metro m ON w.date = m.date
  JOIN inst_final.traffic t ON w.date = t.date
)

SELECT
  weather_type,
  ROUND(AVG(ridership), 0) AS avg_ridership,
  ROUND(AVG(traffic_volume), 0) AS avg_traffic_volume,
  COUNT(*) AS day_count
FROM joined_data
GROUP BY weather_type
ORDER BY weather_type;
```

# 5 Weather conditions on days with most vehicle traffic

```
SELECT 
  t.date,
  t.vol AS traffic_volume,
  w.mean_temp,
  w.max_temp,
  w.min_temp,
  w.precipitation_sum,
  w.weather_code
FROM inst_final.traffic t
JOIN inst_final.weather w ON t.date = w.date
ORDER BY t.vol DESC
LIMIT 10;

```