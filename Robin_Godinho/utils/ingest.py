import requests
import json
import os
from datetime import datetime

# Optional: Make sure data folder exists
RAW_DATA_DIR = "../data/raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def fetch_newsdata(api_key):
    url = f"https://newsdata.io/api/1/news?apikey={api_key}&language=en"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    with open(f"{RAW_DATA_DIR}/newsdata_{datetime.now().isoformat()}.json", "w") as f:
        json.dump(data, f, indent=2)
    return data


def fetch_gnews(api_key):
    url = f"https://gnews.io/api/v4/top-headlines?token={api_key}&lang=en"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    with open(f"{RAW_DATA_DIR}/gnews_{datetime.now().isoformat()}.json", "w") as f:
        json.dump(data, f, indent=2)
    return data


def fetch_reddit(client_id, secret, user_agent, subreddit="news"):
    # Step 1: Get access token
    auth = requests.auth.HTTPBasicAuth(client_id, secret)
    data = {'grant_type': 'client_credentials'}
    headers = {'User-Agent': user_agent}

    res = requests.post("https://www.reddit.com/api/v1/access_token",
                        auth=auth, data=data, headers=headers)
    res.raise_for_status()
    token = res.json()['access_token']

    # Step 2: Use access token to fetch data
    headers['Authorization'] = f"bearer {token}"
    url = f"https://oauth.reddit.com/r/{subreddit}/hot?limit=25"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    with open(f"{RAW_DATA_DIR}/reddit_{datetime.now().isoformat()}.json", "w") as f:
        json.dump(data, f, indent=2)
    return data


# Example usage (Replace with your actual API keys)
if __name__ == "__main__":
    newsdata_api_key = "YOUR_NEWSDATA_API_KEY"
    gnews_api_key = "YOUR_GNEWS_API_KEY"
    reddit_client_id = "YOUR_REDDIT_CLIENT_ID"
    reddit_secret = "YOUR_REDDIT_SECRET"
    reddit_user_agent = "news-sentiment-tracker/0.1 by YOUR_USERNAME"

    fetch_newsdata(newsdata_api_key)
    fetch_gnews(gnews_api_key)
    fetch_reddit(reddit_client_id, reddit_secret, reddit_user_agent)
