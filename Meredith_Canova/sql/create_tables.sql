-- Issue 2
CREATE SCHEMA IF NOT EXISTS `daily_intel_dataset`;
CREATE TABLE IF NOT EXISTS daily_intel_dataset.stocks_daily (
    ticker STRING,
    date DATE,
    open FLOAT64,
    high FLOAT64,
    low FLOAT64,
    close FLOAT64,
    volume INT64,
    daily_range FLOAT64,
    category STRING, 
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_intel_dataset.company_news (
    company STRING,
    ticker STRING,
    title  STRING,
    source STRING,
    published_at TIMESTAMP,
    url STRING, 
    category STRING,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_intel_dataset.google_trends(
    position INT64,
    query STRING,
    search_volume FLOAT64,
    percentage_increase INT64,
    location STRING,
    categories ARRAY<STRING>,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    time_active_minutes FLOAT64,
    keywords ARRAY<STRING>,
    timestamp TIMESTAMP
);
