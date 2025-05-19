# Days with the fewest riders
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

# Average weather conditions for the most rides
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
```

#