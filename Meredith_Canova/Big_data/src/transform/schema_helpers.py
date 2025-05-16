"""
Raw‑to‑clean helpers plus explicit BigQuery schemas.
Used by both cleaner.py and any unit tests.
"""

from datetime import datetime, timezone
from typing import Dict, List
import pandas as pd
import json 

# sector definitions
TECH     = {"AAPL","MSFT","NVDA","AMZN","GOOGL","META"}
FINANCE  = {"JPM","V","MA","BAC","WFC","C"}
HEALTH   = {"UNH","JNJ","PFE","LLY","MRK","ABBV"}
FOOD     = {"PEP","KO","MDLZ","KHC","TSN","SJM"}

# ── Reverse lookup: ticker → category ───────────────────────
TICKER_CATEGORY: dict[str, str] = {}
for t in TECH:     TICKER_CATEGORY[t] = "Technology"
for t in FINANCE:  TICKER_CATEGORY[t] = "Finance"
for t in HEALTH:   TICKER_CATEGORY[t] = "Healthcare"
for t in FOOD:     TICKER_CATEGORY[t] = "Food & Beverage"

# ── Transformer: Stocks ─────────────────────────────────────
def _df_from_stock_raw(payload: Dict) -> pd.DataFrame:
    if not payload.get("results"):
        return pd.DataFrame()

    r = payload["results"][0]
    df = pd.DataFrame(
        [{
            "ticker":  payload["ticker"],
            "date":    datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).date().isoformat(),
            "open":    r["o"],
            "high":    r["h"],
            "low":     r["l"],
            "close":   r["c"],
            "volume":  r["v"],
        }]
    )

    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce", downcast="integer")
    df["daily_range"] = df["high"] - df["low"]
    df["category"] = df["ticker"].map(TICKER_CATEGORY).fillna("Other")
    df["timestamp"] = datetime.now(timezone.utc).isoformat()


    return df.dropna(subset=["ticker", "date"])

# ── Transformer: News ───────────────────────────────────────
def _df_from_news_raw(payload: List[Dict]) -> pd.DataFrame:
    if not payload:
        return pd.DataFrame()

    df = pd.DataFrame(payload).rename(columns={"publishedAt": "published_at"})
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    df = df[df["published_at"].notna()]
    df["published_at"] = df["published_at"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    df["category"] = df["ticker"].map(TICKER_CATEGORY).fillna("Other")
  
    df = df.dropna(subset=["company", "ticker", "title", "published_at"])

    df = df[[
        "company", "ticker", "title", "source", "published_at", "url", "category"
    ]].copy()

    df["company"] = df["company"].astype(str)
    df["ticker"] = df["ticker"].astype(str)
    df["title"] = df["title"].astype(str)
    df["source"] = df["source"].astype(str)
    df["url"] = df["url"].astype(str)
    df["category"] = df["category"].astype(str)
    df["timestamp"] = datetime.now(timezone.utc).isoformat()


    return df

# ── Transformer: Google Trends ──────────────────────────────
def _df_from_trends_raw(payload: Dict) -> pd.DataFrame:
    trends = payload.get("trends", [])
    if not trends:
        return pd.DataFrame()

    df = pd.json_normalize(trends)

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["timestamp"] = datetime.now(timezone.utc).isoformat()

    for col in ["position", "search_volume", "percentage_increase"]:
        df[col] = pd.to_numeric(df[col], errors="coerce", downcast="integer")
    df["time_active_minutes"] = pd.to_numeric(df["time_active_minutes"], errors="coerce")


    for col in ["categories", "keywords"]:
        df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

    return df[[
        "position", "query", "search_volume", "percentage_increase", "location",
        "categories", "start_date", "end_date", "time_active_minutes", "keywords",
        "timestamp"
    ]]



TRANSFORMERS = {
    "stocks":  _df_from_stock_raw,
    "news":    _df_from_news_raw,
    "trends":  _df_from_trends_raw,
}
