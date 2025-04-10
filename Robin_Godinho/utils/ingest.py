import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access API keys securely from .env
newsdata_api_key = os.getenv("NEWSDATA_API_KEY")
gnews_api_key = os.getenv("GNEWS_API_KEY")
mediastack_api_key = os.getenv("MEDIASTACK_API_KEY")

# Ensure raw data directory exists
RAW_DATA_DIR = "data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Optional: Toggle to save with timestamps
USE_TIMESTAMP = False

# Helper: Safe filename timestamp
def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Helper: Generate filename based on flag
def get_filename(source_name):
    if USE_TIMESTAMP:
        return f"{RAW_DATA_DIR}/{source_name}_{get_timestamp()}.json"
    else:
        return f"{RAW_DATA_DIR}/{source_name}_latest.json"

# ---------------------- Fetch Functions ---------------------- #

def fetch_newsdata(api_key):
    url = f"https://newsdata.io/api/1/news?apikey={api_key}&language=en"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    filename = get_filename("newsdata")
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Newsdata saved to {filename}")
    return data

def fetch_gnews(api_key):
    url = f"https://gnews.io/api/v4/top-headlines?token={api_key}&lang=en"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    filename = get_filename("gnews")
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ GNews saved to {filename}")
    return data

def fetch_mediastack(api_key):
    url = f"http://api.mediastack.com/v1/news?access_key={api_key}&languages=en&limit=25"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    filename = get_filename("mediastack")
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Mediastack data saved to {filename}")
    return data

# ---------------------- Main Run Block ---------------------- #

if __name__ == "__main__":
    fetch_newsdata(newsdata_api_key)
    fetch_gnews(gnews_api_key)
    fetch_mediastack(mediastack_api_key)
