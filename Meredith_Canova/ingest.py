import os
import requests
import logging
import pandas as pd
from datetime import datetime

# --- Config Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Constants ---
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
KEYWORDS = [
    "drug shortage", "drug discontinuation", "drug recall", "drug supply issue",
    "medication discontinued", "drug availability", "pharmaceutical halt",
    "fda alert", "drug market removal", "drug unavailability"
]

# --- Utility Function ---
def call_api(url, params=None, headers=None):
    try:
        logging.info(f"Calling API: {url}")
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None

# --- News ---
def fetch_news_articles():
    if not NEWSAPI_KEY:
        raise ValueError("Missing NEWSAPI_KEY in environment.")

    all_articles = []
    for keyword in KEYWORDS:
        params = {
            "q": keyword,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 100,
            "apiKey": NEWSAPI_KEY
        }
        news_data = call_api("https://newsapi.org/v2/everything", params=params)
        if news_data and "articles" in news_data:
            for article in news_data["articles"]:
                article["matched_keyword"] = keyword
                all_articles.append(article)
    
    if all_articles:
        news_df = pd.DataFrame(all_articles)
        news_df["data_pull_date"] = datetime.today().date()
        news_df = news_df.dropna(subset=["title", "publishedAt", "url"]) 
        news_df.to_json("data/news_data.json", orient="records", lines=True)
        logging.info(f"Saved {len(news_df)} news articles.")
    else:
        logging.warning("No news articles fetched.")

# --- FDA Drug Shortages ---
def fetch_drug_shortages():
    data = call_api("https://api.fda.gov/drug/shortages.json", {"limit": 100})
    if data and "results" in data:
        df = pd.DataFrame(data["results"])
        df["data_pull_date"] = datetime.today().date()
        df.to_json("data/drug_shortages.json", orient="records", lines=True)
        logging.info(f"Saved {len(df)} drug shortage records.")
    else:
        logging.warning("No drug shortage data returned.")

# --- FDA Drug Recalls ---
def fetch_drug_recalls():
    data = call_api("https://api.fda.gov/drug/enforcement.json", {"limit": 100})
    if data and "results" in data:
        df = pd.DataFrame(data["results"])
        df["data_pull_date"] = datetime.today().date()
        df.to_json("data/drug_recalls.json", orient="records", lines=True)
        logging.info(f"Saved {len(df)} drug recall records.")
    else:
        logging.warning("No drug recall data returned.")

# --- Main Runner ---
def main():
    logging.info("Starting data fetch pipeline...")
    os.makedirs("data", exist_ok=True)
    fetch_drug_shortages()
    fetch_drug_recalls()
    fetch_news_articles()
    logging.info("All data sources fetched and saved.")

if __name__ == "__main__":
    main()
 