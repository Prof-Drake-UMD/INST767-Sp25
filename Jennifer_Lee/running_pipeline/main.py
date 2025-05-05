from flask import Flask
import yfinance as yf
import pandas as pd
import json
from google.cloud import storage
import requests
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import os

app = Flask(__name__)

def clean_and_export_csv(df, path, date_col=None):
    if date_col and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df[date_col] = df[date_col].dt.date  # convert to YYYY-MM-DD
    df = df.dropna(how="all")  # remove empty rows
    df.to_csv(path, index=False, encoding='utf-8')
    print(f"Cleaned CSV saved to {path}")

def upload_to_gcs(bucket_name, source_file, destination_blob):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")

@app.route('/')
def run_pipeline():
    # Download stock data
    ticker = yf.Ticker("AAPL")
    stock_data = ticker.history(period="1y", interval="1d")
    stock_data.reset_index(inplace=True)
    clean_and_export_csv(stock_data, "/tmp/aapl_stock_data.csv", "Date")
    upload_to_gcs("jennifer-finanical-bucket", "/tmp/aapl_stock_data.csv", "aapl_stock_data.csv")

    # Download crypto data
    crypto_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {"vs_currency": "usd", "days": "365", "interval": "daily"}
    crypto_resp = requests.get(crypto_url, params=params).json()
    prices = crypto_resp["prices"]
    crypto_df = pd.DataFrame(prices, columns=["timestamp", "price_usd"])
    crypto_df["timestamp"] = pd.to_datetime(crypto_df["timestamp"], unit="ms")
    clean_and_export_csv(crypto_df, "/tmp/bitcoin_data.csv", "timestamp")
    upload_to_gcs("jennifer-finanical-bucket", "/tmp/bitcoin_data.csv", "bitcoin_data.csv")

    # Download macro data (FRED)
    fred_api_key = "3ab53daa5812f18f00c5b0873399a5bc"
    fred_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={fred_api_key}&file_type=json"
    fred_resp = requests.get(fred_url).json()
    observations = fred_resp["observations"]
    fred_df = pd.DataFrame(observations)
    fred_df = fred_df[fred_df["value"] != "."]
    fred_df["value"] = fred_df["value"].astype(float)
    fred_df["date"] = pd.to_datetime(fred_df["date"])
    one_year_ago = datetime.today() - timedelta(days=365)
    fred_df = fred_df[fred_df["date"] >= one_year_ago]
    fred_df = fred_df[["date", "value"]]
    clean_and_export_csv(fred_df, "/tmp/macro_indicators.csv", "date")
    upload_to_gcs("jennifer-finanical-bucket", "/tmp/macro_indicators.csv", "macro_indicators.csv")

    # Publish message to Pub/Sub
    project_id = "inst767final"
    topic_id = "data_ready"
    message = "New data ingested and ready for transformation"
    publish_message(project_id, topic_id, message)

    return "Pipeline complete, files cleaned and uploaded!"

def publish_message(project_id, topic_id, message):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    future = publisher.publish(topic_path, message.encode("utf-8"))
    print(f"Published message to {topic_path}: {future.result()}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
