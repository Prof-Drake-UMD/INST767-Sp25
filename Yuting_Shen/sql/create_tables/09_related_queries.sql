-- Create related queries table
CREATE OR REPLACE TABLE sports_analytics.related_queries (
  main_keyword STRING NOT NULL,
  related_query STRING NOT NULL,
  date DATE NOT NULL,
  query_type STRING,  -- 'top' or 'rising'
  interest_value INT64,  -- For top queries: 0-100 scale; for rising queries: percent increase
  created_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY main_keyword;