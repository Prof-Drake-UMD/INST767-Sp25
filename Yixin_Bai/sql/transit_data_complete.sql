-- File: schemas/transit_data_complete.sql

-- =====================================================================
-- ISSUE #2: DATA MODELING FOR BIGQUERY
-- This file contains the SQL statements to create the data model and
-- analytical queries for the transit data integration project
-- =====================================================================

-- Create the dataset if it doesn't exist
-- ISSUE #2: Creating dataset in BigQuery
CREATE SCHEMA IF NOT EXISTS `lithe-camp-458100-s5.transit_data`;

-- ISSUE #2: Table creation statements for BigQuery
-- Create MBTA data table
CREATE OR REPLACE TABLE `lithe-camp-458100-s5.transit_data.mbta_data`
(
  source STRING,
  route_id STRING,
  route_type INT64,
  route_name STRING,
  route_short_name STRING,
  route_color STRING,
  route_text_color STRING,
  route_description STRING,
  prediction_id STRING,
  stop_id STRING,
  vehicle_id STRING,
  direction_id INT64,
  departure_time TIMESTAMP,
  arrival_time TIMESTAMP,
  status STRING,
  timestamp TIMESTAMP
) 
PARTITION BY DATE(timestamp)
CLUSTER BY route_id, timestamp;

-- ISSUE #2: Table creation statements for BigQuery
-- Create WMATA data table
CREATE OR REPLACE TABLE `lithe-camp-458100-s5.transit_data.wmata_data`
(
  source STRING,
  prediction_id STRING,
  station_code STRING,
  station_name STRING,
  train_line STRING,
  destination_code STRING,
  destination_name STRING,
  minutes_to_arrival STRING,
  cars STRING,
  timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp)
CLUSTER BY train_line, timestamp;

-- ISSUE #2: Table creation statements for BigQuery
-- Create CTA train arrivals data table
CREATE OR REPLACE TABLE `lithe-camp-458100-s5.transit_data.cta_data`
(
  source STRING,
  station_id STRING,
  station_name STRING,
  train_run_number STRING,
  route_id STRING,
  destination_name STRING,
  direction STRING,
  prediction_generated_time STRING,
  arrival_time TIMESTAMP,
  is_approaching STRING,
  is_scheduled STRING,
  is_fault STRING,
  is_delayed STRING,
  flags STRING,
  timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp)
CLUSTER BY route_id, timestamp;

-- ISSUE #2: Table creation statements for BigQuery
-- Create CTA train positions data table
CREATE OR REPLACE TABLE `lithe-camp-458100-s5.transit_data.cta_positions_data`
(
  source STRING,
  route_id STRING,
  vehicle_id STRING,
  destination_name STRING,
  direction STRING,
  next_station_id STRING,
  next_station_name STRING,
  predicted_arrival_time TIMESTAMP,
  is_approaching STRING,
  is_delayed STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  heading STRING,
  timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp)
CLUSTER BY route_id, timestamp;

-- ISSUE #2: Table creation statements for BigQuery
-- ISSUE #6: Data integration into final BigQuery table
-- Create integrated transit data table
CREATE OR REPLACE TABLE `lithe-camp-458100-s5.transit_data.integrated_transit_data`
PARTITION BY DATE(recorded_at)
CLUSTER BY transit_system, route_id
AS
WITH mbta_standardized AS (
  SELECT
    'mbta' AS transit_system,
    CAST(route_id AS STRING) AS route_id,
    CAST(stop_id AS STRING) AS station_id,
    CAST(NULL AS STRING) AS station_name,
    CAST(vehicle_id AS STRING) AS vehicle_id,
    CAST(direction_id AS INT64) AS direction_id,
    CAST(arrival_time AS TIMESTAMP) AS arrival_time, 
    CAST(departure_time AS TIMESTAMP) AS departure_time,
    CAST(status AS STRING) AS status,
    CAST(prediction_id AS STRING) AS prediction_id,
    CAST(NULL AS FLOAT64) AS latitude,
    CAST(NULL AS FLOAT64) AS longitude,
    CAST(NULL AS STRING) AS heading,
    'prediction' AS data_type,  
    CAST(timestamp AS TIMESTAMP) AS recorded_at
  FROM `lithe-camp-458100-s5.transit_data.mbta_data`
  WHERE source = 'mbta'
    AND prediction_id IS NOT NULL
),

wmata_standardized AS (
  SELECT
    'wmata' AS transit_system,
    CAST(train_line AS STRING) AS route_id,
    CAST(station_code AS STRING) AS station_id,
    CAST(station_name AS STRING) AS station_name,
    CAST(NULL AS STRING) AS vehicle_id,
    CAST(NULL AS INT64) AS direction_id,
    CASE
        WHEN minutes_to_arrival = 'ARR' THEN timestamp
        WHEN minutes_to_arrival = 'BRD' THEN timestamp
        WHEN SAFE_CAST(minutes_to_arrival AS INT64) IS NOT NULL
            THEN TIMESTAMP_ADD(timestamp, INTERVAL SAFE_CAST(minutes_to_arrival AS INT64) MINUTE)
        ELSE NULL
    END AS arrival_time,
    CAST(NULL AS TIMESTAMP) AS departure_time,
    CAST(NULL AS STRING) AS status,
    CAST(prediction_id AS STRING) AS prediction_id,
    CAST(NULL AS FLOAT64) AS latitude,
    CAST(NULL AS FLOAT64) AS longitude,
    CAST(NULL AS STRING) AS heading,
    'prediction' AS data_type,  
    CAST(timestamp AS TIMESTAMP) AS recorded_at
  FROM `lithe-camp-458100-s5.transit_data.wmata_data`
  WHERE source = 'wmata'
),

cta_predictions_standardized AS (
  SELECT
    'cta' AS transit_system,
    CAST(route_id AS STRING) AS route_id,
    CAST(station_id AS STRING) AS station_id,
    CAST(station_name AS STRING) AS station_name,
    CAST(train_run_number AS STRING) AS vehicle_id,
    CAST(direction AS INT64) AS direction_id,
    CAST(arrival_time AS TIMESTAMP) AS arrival_time,
    CAST(NULL AS TIMESTAMP) AS departure_time,
    CONCAT(
        IF(is_approaching = '1', 'Approaching;', ''),
        IF(is_scheduled = '1', 'Scheduled;', '')
    ) AS status,
    CAST(NULL AS STRING) AS prediction_id,
    CAST(NULL AS FLOAT64) AS latitude,
    CAST(NULL AS FLOAT64) AS longitude,
    CAST(NULL AS STRING) AS heading,
    'prediction' AS data_type,  
    CAST(timestamp AS TIMESTAMP) AS recorded_at
  FROM `lithe-camp-458100-s5.transit_data.cta_data`
  WHERE source = 'cta'
),

cta_positions_standardized AS (
  SELECT
    'cta' AS transit_system,
    CAST(route_id AS STRING) AS route_id,
    CAST(next_station_id AS STRING) AS station_id,  
    CAST(next_station_name AS STRING) AS station_name,  
    CAST(vehicle_id AS STRING) AS vehicle_id,
    CAST(direction AS INT64) AS direction_id,
    CAST(predicted_arrival_time AS TIMESTAMP) AS arrival_time,  
    CAST(NULL AS TIMESTAMP) AS departure_time,
    CONCAT(
        IF(is_approaching = '1', 'Approaching;', ''),
        IF(is_delayed = '1', 'Delayed;', '')
    ) AS status,
    CAST(NULL AS STRING) AS prediction_id,
    CAST(latitude AS FLOAT64) AS latitude,  
    CAST(longitude AS FLOAT64) AS longitude,  
    CAST(heading AS STRING) AS heading, 
    'position' AS data_type,  
    CAST(timestamp AS TIMESTAMP) AS recorded_at
  FROM `lithe-camp-458100-s5.transit_data.cta_positions_data`
  WHERE source = 'cta'
)

SELECT * FROM mbta_standardized
UNION ALL
SELECT * FROM wmata_standardized
UNION ALL
SELECT * FROM cta_predictions_standardized
UNION ALL
SELECT * FROM cta_positions_standardized;

-- ISSUE #2: Creating view to facilitate analytical queries
CREATE OR REPLACE VIEW `lithe-camp-458100-s5.transit_data.transit_analysis_view` AS
SELECT 
  transit_system,
  route_id,
  station_id,
  station_name,
  vehicle_id,
  direction_id,
  arrival_time,
  departure_time,
  status,
  prediction_id,
  latitude,
  longitude,
  heading,
  data_type,
  recorded_at,
  DATE(recorded_at) AS recorded_date,
  EXTRACT(HOUR FROM recorded_at) AS hour_of_day,
  EXTRACT(DAYOFWEEK FROM recorded_at) AS day_of_week,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM recorded_at) BETWEEN 2 AND 6 THEN 'Weekday'
    ELSE 'Weekend'
  END AS day_type,
  CASE
    WHEN EXTRACT(HOUR FROM recorded_at) BETWEEN 6 AND 9 THEN 'Morning Peak'
    WHEN EXTRACT(HOUR FROM recorded_at) BETWEEN 16 AND 19 THEN 'Evening Peak'
    ELSE 'Off-Peak'
  END AS time_period,
  TIMESTAMP_DIFF(arrival_time, recorded_at, MINUTE) AS predicted_wait_minutes
FROM `lithe-camp-458100-s5.transit_data.integrated_transit_data`
WHERE arrival_time IS NOT NULL;

-- ===================================================================================
-- ISSUE #2: ANALYTICAL QUERIES
-- These queries demonstrate the analytical capabilities of the data model
-- ===================================================================================

-- ISSUE #2: Analytical Query 1 - Average wait times by transit system during peak hours
-- This query helps transit planners understand how wait times vary across systems during rush hour
CREATE OR REPLACE VIEW `lithe-camp-458100-s5.transit_data.analysis_wait_times` AS
SELECT
  transit_system,
  time_period,
  COUNT(*) AS total_predictions,
  AVG(predicted_wait_minutes) AS avg_wait_minutes,
  MIN(predicted_wait_minutes) AS min_wait_minutes,
  MAX(predicted_wait_minutes) AS max_wait_minutes,
  STDDEV(predicted_wait_minutes) AS stddev_wait_minutes
FROM
  `lithe-camp-458100-s5.transit_data.transit_analysis_view`
WHERE
  arrival_time > recorded_at
  AND predicted_wait_minutes BETWEEN 0 AND 60 -- Filter out unrealistic wait times
GROUP BY
  transit_system, time_period
ORDER BY
  transit_system, time_period;

-- ISSUE #2: Analytical Query 2 - On-time performance by transit system
-- This helps identify which transit systems are most reliable
CREATE OR REPLACE VIEW `lithe-camp-458100-s5.transit_data.analysis_ontime_performance` AS
WITH prediction_accuracy AS (
  SELECT
    transit_system,
    route_id,
    CASE
      WHEN predicted_wait_minutes <= 1 
        THEN 'On Time (<=1 min)'
      WHEN predicted_wait_minutes BETWEEN 2 AND 5 
        THEN 'Minor Delay (2-5 min)'
      WHEN predicted_wait_minutes BETWEEN 6 AND 10 
        THEN 'Moderate Delay (6-10 min)'
      ELSE 'Significant Delay (>10 min)'
    END AS delay_category,
    COUNT(*) AS prediction_count
  FROM
    `lithe-camp-458100-s5.transit_data.transit_analysis_view`
  WHERE
    data_type = 'prediction'
    AND arrival_time > recorded_at
  GROUP BY
    transit_system, route_id, delay_category
)

SELECT
  transit_system,
  delay_category,
  COUNT(DISTINCT route_id) AS route_count,
  SUM(prediction_count) AS total_predictions,
  ROUND(SUM(prediction_count) * 100.0 / SUM(SUM(prediction_count)) OVER (PARTITION BY transit_system), 2) AS percentage
FROM
  prediction_accuracy
GROUP BY
  transit_system, delay_category
ORDER BY
  transit_system, 
  CASE
    WHEN delay_category = 'On Time (<=1 min)' THEN 1
    WHEN delay_category = 'Minor Delay (2-5 min)' THEN 2
    WHEN delay_category = 'Moderate Delay (6-10 min)' THEN 3
    ELSE 4
  END;

-- ISSUE #2: Analytical Query 3 - Geographic distribution of transit vehicles
-- This helps visualize where transit vehicles are at any given moment
CREATE OR REPLACE VIEW `lithe-camp-458100-s5.transit_data.analysis_vehicle_locations` AS
SELECT
  transit_system,
  route_id,
  vehicle_id,
  station_name AS nearest_station,
  latitude,
  longitude,
  CASE
    WHEN status LIKE '%Approaching%' THEN 'Approaching Station'
    WHEN status LIKE '%Delayed%' THEN 'Delayed'
    ELSE 'In Transit'
  END AS vehicle_status,
  ST_GEOGPOINT(longitude, latitude) AS geo_point, -- Create point for mapping
  recorded_at
FROM
  `lithe-camp-458100-s5.transit_data.integrated_transit_data`
WHERE
  data_type = 'position'
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
  -- Get only the most recent position for each vehicle
  AND recorded_at = (
    SELECT MAX(recorded_at)
    FROM `lithe-camp-458100-s5.transit_data.integrated_transit_data` t2
    WHERE 
      t2.vehicle_id = `lithe-camp-458100-s5.transit_data.integrated_transit_data`.vehicle_id
      AND t2.transit_system = `lithe-camp-458100-s5.transit_data.integrated_transit_data`.transit_system
  );

-- ISSUE #2: Analytical Query 4 - Identification of congestion patterns
-- This helps identify potential bottlenecks in the transit network
CREATE OR REPLACE VIEW `lithe-camp-458100-s5.transit_data.analysis_busy_stations` AS
WITH station_activity AS (
  SELECT
    transit_system,
    station_id,
    station_name,
    hour_of_day,
    day_of_week,
    COUNT(*) AS prediction_count,
    COUNT(DISTINCT vehicle_id) AS unique_vehicles
  FROM
    `lithe-camp-458100-s5.transit_data.transit_analysis_view`
  WHERE
    station_id IS NOT NULL
    AND recorded_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY
    transit_system, station_id, station_name, hour_of_day, day_of_week
),

station_rankings AS (
  SELECT
    transit_system,
    station_id,
    station_name,
    hour_of_day,
    CASE
      WHEN day_of_week BETWEEN 2 AND 6 THEN 'Weekday'
      ELSE 'Weekend'
    END AS day_type,
    SUM(prediction_count) AS total_predictions,
    SUM(unique_vehicles) AS total_vehicles,
    RANK() OVER (
      PARTITION BY transit_system, 
      CASE WHEN day_of_week BETWEEN 2 AND 6 THEN 'Weekday' ELSE 'Weekend' END,
      hour_of_day 
      ORDER BY SUM(prediction_count) DESC
    ) AS station_rank
  FROM
    station_activity
  GROUP BY
    transit_system, station_id, station_name, hour_of_day, day_type
)

SELECT
  transit_system,
  station_name,
  day_type,
  hour_of_day,
  total_predictions,
  total_vehicles,
  CONCAT(transit_system, '-', station_id) AS station_code
FROM
  station_rankings
WHERE
  station_rank <= 5
  AND hour_of_day IN (8, 12, 17); -- Morning rush, midday, evening rush

-- ISSUE #2: Analytical Query 5 - Correlation between time of day and prediction accuracy
-- This helps understand if prediction quality varies by time of day
CREATE OR REPLACE VIEW `lithe-camp-458100-s5.transit_data.analysis_prediction_accuracy` AS
WITH hourly_stats AS (
  SELECT
    transit_system,
    hour_of_day,
    day_type,
    COUNT(*) AS prediction_count,
    AVG(predicted_wait_minutes) AS avg_wait_minutes,
    STDDEV(predicted_wait_minutes) AS stddev_wait_minutes,
    STDDEV(predicted_wait_minutes) / NULLIF(AVG(predicted_wait_minutes), 0) AS coefficient_of_variation
  FROM
    `lithe-camp-458100-s5.transit_data.transit_analysis_view`
  WHERE
    data_type = 'prediction' 
    AND arrival_time > recorded_at
    AND predicted_wait_minutes BETWEEN 0 AND 60
  GROUP BY
    transit_system, hour_of_day, day_type
)

SELECT
  transit_system,
  day_type,
  hour_of_day,
  prediction_count,
  ROUND(avg_wait_minutes, 2) AS avg_wait_minutes,
  ROUND(stddev_wait_minutes, 2) AS stddev_wait_minutes,
  ROUND(coefficient_of_variation, 4) AS prediction_variability,
  CASE
    WHEN coefficient_of_variation < 0.1 THEN 'Very Reliable'
    WHEN coefficient_of_variation < 0.25 THEN 'Reliable'
    WHEN coefficient_of_variation < 0.5 THEN 'Moderately Reliable'
    WHEN coefficient_of_variation < 1.0 THEN 'Less Reliable'
    ELSE 'Unreliable'
  END AS reliability_category
FROM
  hourly_stats
WHERE
  prediction_count > 10; -- Ensure sufficient data points for meaningful statistics