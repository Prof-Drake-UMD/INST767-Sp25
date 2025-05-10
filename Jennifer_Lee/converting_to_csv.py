import json
import pandas as pd
import os


os.makedirs("output/csv", exist_ok=True)

def convert_stock_json_to_csv():
    with open("output/json/aapl_stock_data.json") as f:
        stock_json = json.load(f)

    stock_df = pd.DataFrame(stock_json)
    print("Columns in stock_df:", stock_df.columns) 

    stock_df["ticker"] = "AAPL"
    stock_df = stock_df[["ticker", "Date", "Open", "High", "Low", "Close", "Volume"]]
    stock_df.to_csv("output/csv/stock_prices.csv", index=False)
    print("Converted stock_prices.csv")

def convert_crypto_json_to_csv():

    with open("output/json/bitcoin_data.json")s as f:
        crypto_json = json.load(f)

    crypto_df = pd.DataFrame(crypto_json)


    crypto_df["timestamp"] = pd.to_datetime(crypto_df["timestamp"])
    crypto_df["crypto_id"] = crypto_df["crypto_id"].fillna("bitcoin")

    crypto_df = crypto_df[["crypto_id", "timestamp", "price_usd"]]


    crypto_df.to_csv("output/csv/crypto_prices.csv", index=False)
    print("Converted crypto_prices.csv (1 year of Bitcoin prices)")

from datetime import datetime, timedelta

def convert_fred_json_to_csv():
    with open("output/json/cpi_data.json") as f:
        fred_json = json.load(f)

    observations = fred_json["observations"]
    macro_df = pd.DataFrame(observations)
    macro_df = macro_df[macro_df["value"] != "."] 
    macro_df["value"] = macro_df["value"].astype(float)

    # Convert date and filter for 1 year
    macro_df["date"] = pd.to_datetime(macro_df["date"])
    one_year_ago = datetime.today() - timedelta(days=365)
    macro_df = macro_df[macro_df["date"] >= one_year_ago]

    # Final formatting
    macro_df.insert(0, "series_id", "CPIAUCSL")
    macro_df = macro_df[["series_id", "date", "value"]]

    # Save
    macro_df.to_csv("output/csv/macro_indicators.csv", index=False)
    print("Converted macro_indicators.csv (1 year only)")

# --- Main ---
if __name__ == "__main__":
    convert_stock_json_to_csv()
    convert_crypto_json_to_csv()
    convert_fred_json_to_csv()
