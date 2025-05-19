import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import uuid

# Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

# API Keys
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
SEARCH_API_KEY = os.getenv("SEARCH_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")


# Validate Keys
if not POLYGON_API_KEY:
    raise EnvironmentError("Missing POLYGON_API_KEY")
if not SEARCH_API_KEY:
    raise EnvironmentError("Missing SEARCH_API_KEY")
if not NEWS_API_KEY:
    raise EnvironmentError("Missing NEWS_API_KEY")

# Company list
companies = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "NVDA": "NVIDIA",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "META": "Meta Platforms",
    "JPM":  "JPMorgan Chase",
    "V":    "Visa",
    "MA":   "Mastercard",
    "BAC":  "Bank of America",
    "WFC":  "Wells Fargo",
    "C":    "Citigroup",
    "UNH":  "UnitedHealth Group",
    "JNJ":  "Johnson & Johnson",
    "PFE":  "Pfizer",
    "LLY":  "Eli Lilly",
    "MRK":  "Merck",
    "ABBV": "AbbVie",
    "PEP":  "PepsiCo",
    "KO":   "Coca-Cola",
    "MDLZ": "Mondelez International",
    "KHC":  "Kraft Heinz",
    "TSN":  "Tyson Foods",
    "SJM":  "J.M. Smucker",
}
tickers = list(companies.keys())


# API call helper
def call_api(url, params=None):
    try:
        logging.info(f"Calling API: {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        return None

def fetch_previous_close(tickers):
    records = []
    for ticker in tickers:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
        params = {"apiKey": POLYGON_API_KEY}
        data = call_api(url, params)
        if data and data.get("results"):
            r = data["results"][0]
            records.append({
                "ticker": ticker,
                "date": datetime.fromtimestamp(r["t"] / 1000).date().isoformat(),
                "open": r["o"],
                "high": r["h"],
                "low": r["l"],
                "close": r["c"],
                "volume": r["v"]
            })
        else:
            logging.warning(f"No data for {ticker}")
        time.sleep(15)
    return pd.DataFrame(records)

def fetch_news_today(companies):
    all_articles = []
    start_date = (datetime.now() - timedelta(days=1)).date().isoformat()

    for symbol, name in companies.items():
        logging.info(f"Fetching news for {name}")
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": name,
            "from": start_date,
            "sortBy": "publishedAt",
            "apiKey": NEWS_API_KEY,
            "language": "en",
            "pageSize": 100 
        }
        data = call_api(url, params)
        if data and data.get("articles"):
            for article in data["articles"]:
                all_articles.append({
                    "company": name,
                    "ticker": symbol,
                    "title": article["title"],
                    "source": article["source"]["name"],
                    "published_at": article["publishedAt"],
                    "url": article["url"]
                })
        else:
            logging.warning(f"No articles for {name}")
        time.sleep(2)
    return pd.DataFrame(all_articles)

def fetch_google_trends_24h():
    url = "https://www.searchapi.io/api/v1/search"
    params = {
        "engine": "google_trends_trending_now",
        "geo": "US",
        "api_key": SEARCH_API_KEY
    }
    data = call_api(url, params)
    if not data or not isinstance(data.get("trends"), list):
        logging.warning("No SearchAPI data")
        return pd.DataFrame()
    records = []
    for item in data["trends"]:
        records.append({
            "position": item.get("position"),
            "query": item.get("query"),
            "search_volume": item.get("search_volume"),
            "percentage_increase": item.get("percentage_increase"),
            "location": item.get("location"),
            "categories": item.get("categories"),
            "start_date": item.get("start_date"),
            "end_date": item.get("end_date"),
            "time_active_minutes": item.get("time_active_minutes"),
            "keywords": item.get("keywords"),
        })
    return pd.DataFrame(records)

def save_json(df, name, date_str):
    os.makedirs("local_run/raw", exist_ok=True)
    path = f"local_run/raw/{name}_{date_str}.json"
    df.to_json(path, orient="records", lines=True)
    logging.info(f"Saved {len(df)} records to {path}")

# ── Main Entry ────────────────────────────────────────
def main():
    date_str = datetime.now().date().isoformat()

    df_stocks = fetch_previous_close(tickers)
    save_json(df_stocks, "stocks_daily", date_str)

    df_news = fetch_news_today(companies)
    save_json(df_news, "company_news", date_str)

    df_trends = fetch_google_trends_24h()
    save_json(df_trends, "google_trends", date_str)

    logging.info("All ingestion complete.")


if __name__ == "__main__":
    main()