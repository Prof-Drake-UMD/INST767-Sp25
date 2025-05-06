
CREATE TABLE financial_data.stock_prices (
  ticker STRING,
  date DATE,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64,
  volume FLOAT64
);


CREATE TABLE financial_data.crypto_prices (
  crypto_id STRING,
  timestamp TIMESTAMP,
  price_usd FLOAT64
);

CREATE TABLE financial_data.macro_indicators (
  series_id STRING,
  date DATE,
  value FLOAT64
);
