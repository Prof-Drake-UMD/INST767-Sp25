import os
import json
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List

# ── Ticker to sector/category map ────────────────────────────
TECH     = {"AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META"}
FINANCE  = {"JPM", "V", "MA", "BAC", "WFC", "C"}
HEALTH   = {"UNH", "JNJ", "PFE", "LLY", "MRK", "ABBV"}
FOOD     = {"PEP", "KO", "MDLZ", "KHC", "TSN", "SJM"}

TICKER_CATEGORY: dict[str, str] = {}
for t in TECH:     TICKER_CATEGORY[t] = "Technology"
for t in FINANCE:  TICKER_CATEGORY[t] = "Finance"
for t in HEALTH:   TICKER_CATEGORY[t] = "Healthcare"
for t in FOOD:     TICKER_CATEGORY[t] = "Food & Beverage"

# ── Cleaning Functions ───────────────────────────────────────

def clean_stocks(file_path: str) -> pd.DataFrame:
    with open(file_path) as f:
        raw_data = [json.loads(line) for line in f]

    df = pd.DataFrame(raw_data)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce", downcast="integer")

    df["daily_range"] = df["high"] - df["low"]
    df["category"] = df["ticker"].map(TICKER_CATEGORY).fillna("Other")
    df["timestamp"] = datetime.now(timezone.utc)

    return df.dropna(subset=["ticker", "date"])

def clean_news(file_path: str) -> pd.DataFrame:
    with open(file_path) as f:
        raw_data = [json.loads(line) for line in f]

    df = pd.DataFrame(raw_data)
    if df.empty:
        return df

    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["category"] = df["ticker"].map(TICKER_CATEGORY).fillna("Other")
    df["timestamp"] = datetime.now(timezone.utc)

    df = df.dropna(subset=["company", "ticker", "title", "published_at"])

    df = df[[
        "company", "ticker", "title", "source", "published_at", "url", "category", "timestamp"
    ]].copy()

    return df

def clean_trends(file_path: str) -> pd.DataFrame:
    with open(file_path) as f:
        raw_data = [json.loads(line) for line in f]

    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["timestamp"] = datetime.now(timezone.utc)
    df["time_active_minutes"] = pd.to_numeric(df["time_active_minutes"], errors="coerce")
    for col in ["position", "search_volume", "percentage_increase"]:
        df[col] = pd.to_numeric(df[col], errors="coerce", downcast="integer")
    for col in ["categories", "keywords"]:
        df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

    return df[[
        "position", "query", "search_volume", "percentage_increase", "location",
        "categories", "start_date", "end_date", "time_active_minutes", "keywords",
        "timestamp"
    ]]

# ── File Match Helper ────────────────────────────────────────
def find_file(name_substring: str, raw_dir: str) -> str | None:
    for fname in os.listdir(raw_dir):
        if name_substring in fname and fname.endswith(".json"):
            return os.path.join(raw_dir, fname)
    return None

# ── Run Cleaning and Save ────────────────────────────────────
def process_and_save(name: str, cleaner_func, raw_dir: str, out_dir: str):
    file_path = find_file(name, raw_dir)
    if not file_path:
        print(f"No file found for {name}")
        return

    print(f"Cleaning {name} from {file_path}...")
    df = cleaner_func(file_path)
    if df.empty:
        print(f"Cleaned {name} is empty.")
        return

    out_path = os.path.join(out_dir, f"{name}.csv")
    df.to_csv(out_path, index=False)
    print(f"Saved cleaned {name} → {out_path} ({len(df)} rows)")

# ── Main Entry Point ─────────────────────────────────────────
def main():
    RAW_DIR = "local_run/raw"
    OUT_DIR = "local_run/outputs"
    os.makedirs(OUT_DIR, exist_ok=True)

    process_and_save("stocks_daily", clean_stocks, RAW_DIR, OUT_DIR)
    process_and_save("company_news", clean_news, RAW_DIR, OUT_DIR)
    process_and_save("google_trends", clean_trends, RAW_DIR, OUT_DIR)

    print("Local transformation complete.")

# ── Support both direct run and import ───────────────────────
if __name__ == "__main__":
    main()

