-- Query 1: Shortest-lived trending topic today
-- Purpose: Find the trend that was active for the least amount of time today.
SELECT
  query,
  categories,
  time_active_minutes,
  search_volume,
  start_date, 
  end_date
FROM
  `daily_intel_dataset.google_trends`
WHERE
  DATE(start_date) = CURRENT_DATE()
  AND time_active_minutes IS NOT NULL
ORDER BY
  time_active_minutes ASC
LIMIT 1;


-- Query 2: Top news source by category
-- Purpose: Identify which source published the most articles in each news category.
SELECT
  category,
  source,
  COUNT(*) AS article_count
FROM
  `daily_intel_dataset.company_news`
GROUP BY
  category, source
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) = 1;

-- Query 3: Companies with the most news coverage and their stock volatility
-- Purpose: Compare news volume per company with price volatility and percent change.
WITH news_counts AS (
  SELECT
    ticker, company, COUNT(*) AS article_count
  FROM
    `daily_intel_dataset.company_news`
  WHERE DATE(published_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY ticker, company
),
stock_ranges AS (
  SELECT
    ticker, (high - low) AS daily_range, open, close, volume
  FROM
    `daily_intel_dataset.stocks_daily`
  WHERE DATE(date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)
SELECT
  n.company, n.article_count, s.daily_range, s.volume,
  ROUND(((s.close - s.open) / s.open) * 100, 2) AS percent_change
FROM
  news_counts n
JOIN
  stock_ranges s
ON
  n.ticker = s.ticker
ORDER BY
  n.article_count DESC
LIMIT 10;

-- Query 4: News coverage vs. market performance by category
-- Purpose: Rank each category by news volume and average percent stock change.
WITH news_ranked AS (
  SELECT
    category,
    COUNT(*) AS num_articles,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS news_rank
  FROM
    `daily_intel_dataset.company_news`
  WHERE DATE(published_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY category
),
stock_ranked AS (
  SELECT
    category,
    ROUND(AVG((close - open) / open) * 100, 2) AS avg_percent_change,
    RANK() OVER (ORDER BY AVG((close - open) / open) DESC) AS perf_rank
  FROM
    `daily_intel_dataset.stocks_daily`
  WHERE DATE(date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  GROUP BY category
)
SELECT
  s.category,
  s.avg_percent_change,
  s.perf_rank,
  n.num_articles,
  n.news_rank,
  ABS(s.perf_rank - n.news_rank) AS rank_mismatch
FROM
  stock_ranked s
JOIN
  news_ranked n
ON
  s.category = n.category
ORDER BY
  rank_mismatch DESC, n.num_articles DESC;

-- Query 5: Trending search topics linked to stock market categories
-- Purpose: Match trending queries with stocks in the same category and rank by price movement.
SELECT
  t.query,
  s.ticker,
  s.category AS stock_category,
  ROUND(((s.close - s.open) / s.open) * 100, 2) AS percent_change
FROM
  `daily_intel_dataset.google_trends` t
JOIN
  `daily_intel_dataset.stocks_daily` s
ON
  LOWER(s.category) IN UNNEST(ARRAY(SELECT LOWER(TRIM(c)) FROM UNNEST(t.categories) AS c))
WHERE
  DATE(t.start_date) = CURRENT_DATE()
  AND DATE(s.date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY
  ABS((s.close - s.open) / s.open) DESC;
