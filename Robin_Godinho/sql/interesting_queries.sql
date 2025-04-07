-- 1. This selects the top 10 most positive sources
SELECT 
  source,
  AVG(title_sentiment) AS avg_title_sentiment,
  AVG(description_sentiment) AS avg_description_sentiment,
  COUNT(*) AS article_count
FROM `real-news-analytics.real_news_dataset.news_articles`
GROUP BY source
ORDER BY avg_title_sentiment DESC
LIMIT 10;


-- 2. Sentiment trend over time (daily average sentiment)
SELECT
  DATE(published_at) AS date,
  AVG(title_sentiment) AS avg_title_sentiment,
  AVG(description_sentiment) AS avg_description_sentiment,
  COUNT(*) AS article_count
FROM `real-news-analytics.real_news_dataset.news_articles`
GROUP BY date
ORDER BY date DESC;

-- 3. Most negative headlines in the past 7 days
SELECT
  title,
  title_sentiment,
  published_at
FROM `real-news-analytics.real_news_dataset.news_articles`
WHERE published_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY title_sentiment ASC
LIMIT 2;

-- 4. Source with highest sentiment volatility (standard deviation)
SELECT
  source,
  STDDEV(title_sentiment) AS title_sentiment_stddev,
  STDDEV(description_sentiment) AS desc_sentiment_stddev
FROM `real-news-analytics.real_news_dataset.news_articles`
GROUP BY source
ORDER BY title_sentiment_stddev DESC
LIMIT 3;

-- 5. Daily article volume by source (activity tracker)
SELECT
  DATE(published_at) AS date,
  source,
  COUNT(*) AS article_count
FROM `real-news-analytics.real_news_dataset.news_articles`
GROUP BY date, source
ORDER BY date DESC, article_count DESC;

