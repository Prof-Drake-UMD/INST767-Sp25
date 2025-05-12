-- 1. How does Apple's stock price change align with monthly inflation (CPI)?
SELECT 
  s.date,
  s.close AS aapl_close,
  m.date AS cpi_date,
  m.value AS cpi_value
FROM financial_data.stock_prices s
JOIN financial_data.macro_indicators m
  ON EXTRACT(YEAR FROM s.date) = EXTRACT(YEAR FROM m.date)
     AND EXTRACT(MONTH FROM s.date) = EXTRACT(MONTH FROM m.date)
WHERE s.ticker = 'AAPL'
ORDER BY s.date;

-- 2. What is the daily price change for Apple stock?
SELECT 
  date,
  open,
  close,
  ROUND(close - open, 2) AS daily_change
FROM financial_data.stock_prices
WHERE ticker = 'AAPL'
ORDER BY date;

-- 3. How does Bitcoin price move withs monthly inflation (CPI)?
SELECT 
  DATE(c.timestamp) AS date,
  c.price_usd,
  m.date AS cpi_date,
  m.value AS cpi_value
FROM financial_data.crypto_prices c
JOIN financial_data.macro_indicators m
  ON EXTRACT(YEAR FROM c.timestamp) = EXTRACT(YEAR FROM m.date)
     AND EXTRACT(MONTH FROM c.timestamp) = EXTRACT(MONTH FROM m.date)
WHERE c.crypto_id = 'bitcoin'
ORDER BY date;

-- 4. On what day was AAPL’s highest closing price this year?
SELECT 
  date,
  close
FROM financial_data.stock_prices
WHERE ticker = 'AAPL'
ORDER BY close DESC
LIMIT 1;

-- 5. How do AAPL and Bitcoin prices compare by month?
SELECT
  EXTRACT(YEAR FROM s.date) AS year,
  EXTRACT(MONTH FROM s.date) AS month,
  ROUND(AVG(s.close), 2) AS avg_aapl_close,
  ROUND(AVG(c.price_usd), 2) AS avg_bitcoin_price
FROM financial_data.stock_prices s
JOIN financial_data.crypto_prices c
  ON EXTRACT(YEAR FROM s.date) = EXTRACT(YEAR FROM c.timestamp)
     AND EXTRACT(MONTH FROM s.date) = EXTRACT(MONTH FROM c.timestamp)
WHERE s.ticker = 'AAPL' AND c.crypto_id = 'bitcoin'
GROUP BY year, month
ORDER BY year, month;
