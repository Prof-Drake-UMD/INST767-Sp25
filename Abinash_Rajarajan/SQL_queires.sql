-- Query 1: Top 5 ZIPs by Event & Business Volume

SELECT
  e.postalCode                 AS zip,
  COUNT(*)                     AS event_count,
  COUNT(DISTINCT b.name)       AS business_count,
  ROUND(AVG(b.rating),2)       AS avg_rating
FROM
  `inst767-459621.Location_Optimizer_Analytics.events`   AS e
LEFT JOIN
  `inst767-459621.Location_Optimizer_Analytics.business` AS b
  ON b.zip_code = e.postalCode
GROUP BY
  e.postalCode
ORDER BY
  event_count DESC,
  business_count DESC
LIMIT 5;

-- Query 2: Opportunity Score = Events × Avg Business Rating

WITH zip_metrics AS (
  SELECT
    postalCode                  AS zip,
    COUNT(*)                    AS event_count,
    ROUND(AVG(b.rating),2)      AS avg_rating
  FROM
    `inst767-459621.Location_Optimizer_Analytics.events`   AS e
  LEFT JOIN
    `inst767-459621.Location_Optimizer_Analytics.business` AS b
    ON b.zip_code = e.postalCode
  GROUP BY
    postalCode
)
SELECT
  zip,
  event_count,
  avg_rating,
  event_count * avg_rating  AS opportunity_score
FROM
  zip_metrics
ORDER BY
  opportunity_score DESC
LIMIT 5;

-- Query 3: Business Categories vs. Event Count in All ZIPs

WITH biz_and_events AS (
  SELECT
    b.name          AS biz_name,
    b.zip_code      AS zip,
    COUNT(e.name)   AS event_count,
    category
  FROM
    `inst767-459621.Location_Optimizer_Analytics.business` AS b
  CROSS JOIN
    UNNEST(SPLIT(b.categories, ', ')) AS category
  LEFT JOIN
    `inst767-459621.Location_Optimizer_Analytics.events`   AS e
    ON e.postalCode = b.zip_code
  GROUP BY
    b.name, b.zip_code, category
)
SELECT
  category,
  COUNT(DISTINCT biz_name)  AS num_businesses,
  SUM(event_count)         AS num_events
FROM
  biz_and_events
GROUP BY
  category
ORDER BY
  num_businesses DESC
LIMIT 10;

-- Query 4: Top Categories by Avg Rating (min 5 businesses)

WITH exploded AS (
  SELECT
    b.name,
    b.rating,
    TRIM(category) AS category
  FROM
    `inst767-459621.Location_Optimizer_Analytics.business` AS b,
    UNNEST(SPLIT(b.categories, ',')) AS category
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
  num_businesses >= 5
ORDER BY
  avg_rating DESC,
  num_businesses DESC
LIMIT 10;

-- Query 5: Top Categories by Avg Review Count (min 3 businesses)

SELECT
  TRIM(category)                        AS business_category,
  COUNT(*)                              AS num_businesses,
  ROUND(AVG(review_count), 1)           AS avg_review_count,
  ROUND(STDDEV_POP(review_count), 1)    AS review_count_stddev
FROM
  `inst767-459621.Location_Optimizer_Analytics.business` AS b,
  UNNEST(SPLIT(b.categories, ', '))     AS category
GROUP BY
  category
HAVING
  num_businesses >= 3
ORDER BY
  avg_review_count DESC,
  num_businesses DESC
LIMIT 10;