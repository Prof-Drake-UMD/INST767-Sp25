-- Here are 5 non-trivial queries that can  be run on the combined dataset 

-- 1. 🌡️ Do hotter states emit less carbon per MWh on average? 
SELECT
  w.state,
  AVG(w.temp_celsius) AS avg_temp,
  AVG(c.carbon_kg / c.electricity_value_mwh) AS kg_per_mwh
FROM `ecofusion.weather_snapshot` w
JOIN `ecofusion.carbon_emissions` c ON LOWER(w.state) = LOWER(c.state)
GROUP BY w.state
ORDER BY kg_per_mwh DESC;

-- 2. ⚡ Which state has the highest carbon intensity from coal?
SELECT
  state,
  year,
  coal_emissions,
  total_emissions,
  ROUND(coal_emissions / total_emissions * 100, 2) AS coal_percent
FROM `ecofusion.state_emission_summary`
WHERE total_emissions > 0
ORDER BY coal_percent DESC
LIMIT 10;

-- 3. 📉 Detect states where carbon intensity per MWh deviates the most from national average 
WITH baseline AS (
  SELECT AVG(carbon_kg / electricity_value_mwh) AS national_avg
  FROM `ecofusion.carbon_emissions`
)
SELECT
  c.state,
  ROUND(AVG(c.carbon_kg / c.electricity_value_mwh), 2) AS state_avg,
  ROUND(b.national_avg, 2) AS national_avg,
  ROUND(AVG(c.carbon_kg / c.electricity_value_mwh) - b.national_avg, 2) AS deviation
FROM `ecofusion.carbon_emissions` c, baseline b
GROUP BY c.state, b.national_avg
ORDER BY deviation DESC;


-- 4. ☀️ Are clear sky days associated with lower emissions per state?
SELECT
  w.state,
  COUNTIF(w.weather_main = 'Clear') AS clear_days,
  AVG(c.carbon_kg) AS avg_emission_all_days,
  AVG(CASE WHEN w.weather_main = 'Clear' THEN c.carbon_kg END) AS avg_emission_clear_days
FROM `ecofusion.weather_snapshot` w
JOIN `ecofusion.carbon_emissions` c ON LOWER(w.state) = LOWER(c.state)
GROUP BY w.state
ORDER BY avg_emission_clear_days;

-- 5. 📊 Trend: Top 5 coal-emitting states over years
SELECT
  state,
  year,
  coal_emissions
FROM `ecofusion.state_emission_summary`
WHERE state IN (
  SELECT state FROM `ecofusion.state_emission_summary`
  GROUP BY state
  ORDER BY SUM(coal_emissions) DESC
  LIMIT 5
)
ORDER BY state, year;