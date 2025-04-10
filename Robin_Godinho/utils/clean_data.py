import os
import json
import pandas as pd
import re

RAW_DATA_DIR = "data/raw"
CLEANED_DATA_DIR = "data/cleaned"
os.makedirs(CLEANED_DATA_DIR, exist_ok=True)

def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"http\S+", "", text)                      # Remove URLs
    text = re.sub(r"[^A-Za-z0-9\s]", "", text)               # Remove special characters
    return text.lower().strip()

def load_newsdata(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    articles = data.get("results", [])
    cleaned = []
    for article in articles:
        cleaned.append({
            "source": "newsdata",
            "title": clean_text(article.get("title")),
            "description": clean_text(article.get("description")),
            "published_at": article.get("pubDate")
        })
    return pd.DataFrame(cleaned)

def load_gnews(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    articles = data.get("articles", [])
    cleaned = []
    for article in articles:
        cleaned.append({
            "source": "gnews",
            "title": clean_text(article.get("title")),
            "description": clean_text(article.get("description")),
            "published_at": article.get("publishedAt")
        })
    return pd.DataFrame(cleaned)

def load_mediastack(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    articles = data.get("data", [])
    cleaned = []
    for article in articles:
        cleaned.append({
            "source": "mediastack",
            "title": clean_text(article.get("title")),
            "description": clean_text(article.get("description")),
            "published_at": article.get("published_at")
        })
    return pd.DataFrame(cleaned)

def clean_all():
    dataframes = []

    # Look for _latest.json files
    files = os.listdir(RAW_DATA_DIR)
    sources = {
        "newsdata": load_newsdata,
        "gnews": load_gnews,
        "mediastack": load_mediastack
    }

    for source, loader in sources.items():
        matching_file = next((f for f in files if f.startswith(source) and f.endswith("_latest.json")), None)
        if matching_file:
            file_path = os.path.join(RAW_DATA_DIR, matching_file)
            df = loader(file_path)
            dataframes.append(df)

    if dataframes:
        combined = pd.concat(dataframes, ignore_index=True)
        cleaned_path = os.path.join(CLEANED_DATA_DIR, "cleaned_news.csv")
        combined.to_csv(cleaned_path, index=False)
        print(f"✅ Cleaned data saved to {cleaned_path}")
        return combined
    else:
        print("⚠️ No matching latest files found.")
        return pd.DataFrame()

if __name__ == "__main__":
    clean_all()
