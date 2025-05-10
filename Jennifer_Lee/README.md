Final Project: Automated Financial Data Pipeline on Google Cloud

Project Overview:

This project builds a fully automated data pipeline using Google Cloud Platform (GCP). The system collects, cleans, and stores financial market data from multiple public APIs, transforming it into a usable format for analysis in BigQuery.

For this project, I created a DAG to integrate three different APIs that continuously update financial and economic data. These APIs provide stock market data, cryptocurrency pricing, and macroeconomic indicators. The goal is to analyze relationships between traditional financial markets, cryptocurrency trends, and broader economic factors. I decided to focus on Apple Stock, Bitcoin, and CPIAUCSL for this project. 


Data Sources:

1. Yahoo Finance (https://github.com/ranaroussi/yfinance)  
   - Input: Ticker symbols  
   - Output: Historical prices, volume, and dividend information for stocks  

2. CoinGecko (https://publicapis.io/coin-gecko-api)  
   - Input: Cryptocurrency IDs  
   - Output: Price in USD, market cap, and trading volume for cryptocurrencies  

3. Federal Reserve Economic Data (FRED) (https://publicapis.io/fred-api)  
   - Input: Series ID (e.g., CPIAUCSL for Consumer Price Index)  
   - Output: Time-series macroeconomic indicators  

Technologies Used:

Google Cloud Run for ingestion and transformation services  
Google Cloud Storage for intermediate raw file storage  
Google Pub/Sub for pipeline orchestration  
BigQuery for final structured data storage  
Python, Flask, pandas, requests  


How It Works:

1. Run Pipeline Service  
   - Fetches data from the three APIs  
   - Cleans and exports CSVs  
   - Uploads to Cloud Storage  
   - Publishes a message to a Pub/Sub topic  

2. Transform Service  
   - Triggered by the Pub/Sub topic  
   - Downloads files from Cloud Storage  
   - Applies final transformations  
   - Loads clean data into BigQuery  

Final Output Tables (BigQuery):

financial_data.aapl_stock  
financial_data.bitcoin_prices  
financial_data.macro_indicators  

