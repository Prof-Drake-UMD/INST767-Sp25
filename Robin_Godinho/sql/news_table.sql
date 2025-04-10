CREATE TABLE `your-project-id.real_news_dataset.news_articles` (
  article_id STRING, -- optional, add if you generate unique IDs
  source STRING,
  title STRING,
  description STRING,
  published_at TIMESTAMP,
  title_sentiment FLOAT64,
  description_sentiment FLOAT64
);
