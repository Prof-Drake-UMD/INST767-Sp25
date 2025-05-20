--Query1- Top 5 event hotspots 

SELECT
  e.postalCode                 AS zip,
  COUNT(*)                     AS event_count
FROM
  `inst767-459621.Location_Optimizer_Analytics.events`   e
LEFT JOIN
  `inst767-459621.Location_Optimizer_Analytics.business` b
  ON b.zip_code = e.postalCode
GROUP BY
  e.postalCode
ORDER BY
  event_count DESC
LIMIT 5;

--Query 2 - Opportunity Score - Event Traffic * Avg Business rating

SELECT
  postalCode AS zip, -- ZIP code
  COUNT(*) AS event_count, -- Count of events in the ZIP code
  ROUND(AVG(5.0), 2) AS avg_rating, -- Placeholder for avg_rating (assuming a default rating of 5.0)
  COUNT(*) * ROUND(AVG(5.0), 2) AS opportunity_score -- Opportunity score calculation
FROM
  `inst767-459621.Location_Optimizer_Analytics.events`
WHERE
  postalCode IS NOT NULL -- Ensure ZIP codes are valid
GROUP BY
  postalCode
ORDER BY
  opportunity_score DESC -- Sort by the highest opportunity score
LIMIT 5; -- Return the top 5 ZIP codes

--Query 3 - Business Category Distribution in Top Event Zip codes

WITH biz_and_events AS (
  SELECT
    b.name       AS biz_name,
    b.zip_code   AS zip,
    COUNT(e.name) AS event_count,
    category
  FROM
    `inst767-459621.Location_Optimizer_Analytics.business` b
  CROSS JOIN
    UNNEST(SPLIT(b.categories, ', ')) AS category
  LEFT JOIN
    `inst767-459621.Location_Optimizer_Analytics.events` e
    ON e.postalCode = b.zip_code
  GROUP BY b.name, b.zip_code, category
)
SELECT
  category,
  COUNT(DISTINCT biz_name) AS num_businesses
FROM biz_and_events
GROUP BY category
ORDER BY num_businesses DESC
LIMIT 10;


--Query 4- Top Categories by AVg Rating (min 5 businesses)

-- Name: Top Categories by Avg Rating (min 5 businesses)
WITH exploded AS (
  -- 1) Split each business into one row per category
  SELECT
    b.name,
    b.rating,
    TRIM(category) AS category
  FROM
    `inst767-459621.Location_Optimizer_Analytics.business` b,
    UNNEST(SPLIT(b.categories, ",")) AS category
)

SELECT
  category,
  COUNT(*)                     AS num_businesses,
  ROUND(AVG(rating), 2)        AS avg_rating,
  ROUND(STDDEV_POP(rating), 2) AS rating_stddev
FROM
  exploded
GROUP BY
  category
HAVING
  num_businesses >= 5          -- only categories with a decent sample size
ORDER BY
  avg_rating DESC,
  num_businesses DESC
LIMIT 10;

--Query 5 - Top Business Categories by Average Review Count

-- Top Business Categories by Average Review Count

SELECT
  TRIM(category) AS business_category,
  COUNT(*) AS num_businesses,
  ROUND(AVG(review_count), 1) AS avg_review_count,
  w.temperature AS temperature -- Directly display the temperature from the weather table
FROM
  `inst767-459621.Location_Optimizer_Analytics.business` b,
  UNNEST(SPLIT(b.categories, ', ')) AS category
LEFT JOIN
  `inst767-459621.Location_Optimizer_Analytics.weather` w
  ON ABS(b.latitude - w.latitude) < 0.1 -- Match latitude with a small tolerance
     AND ABS(b.longitude - w.longitude) < 0.1 -- Match longitude with a small tolerance
GROUP BY
  category, w.temperature -- Include temperature in the GROUP BY clause
HAVING
  num_businesses >= 3 -- Filter out tiny niches
ORDER BY
  avg_review_count DESC,
  num_businesses DESC
LIMIT 10;

