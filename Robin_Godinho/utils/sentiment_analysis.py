import os
import pandas as pd
from textblob import TextBlob

# File paths
INPUT_FILE = "data/cleaned/cleaned_news.csv"
OUTPUT_FILE = "data/cleaned/cleaned_news_with_sentiment.csv"

# Ensure cleaned data directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

def get_sentiment(text):
    if pd.isna(text):
        return 0.0
    return TextBlob(text).sentiment.polarity  # Ranges from -1 (neg) to 1 (pos)

def analyze_sentiment():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return

    # Load the cleaned news
    df = pd.read_csv(INPUT_FILE)

    # Apply sentiment analysis
    df["title_sentiment"] = df["title"].apply(get_sentiment)
    df["description_sentiment"] = df["description"].apply(get_sentiment)

    # Save the result
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Sentiment analysis complete! Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    analyze_sentiment()
