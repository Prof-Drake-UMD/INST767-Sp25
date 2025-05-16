from google.cloud import bigquery

# ── BigQuery schemas ───────────────────────────────────────────
stocks_schema = [
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("open", "FLOAT"),
    bigquery.SchemaField("high", "FLOAT"),
    bigquery.SchemaField("low", "FLOAT"),
    bigquery.SchemaField("close", "FLOAT"),
    bigquery.SchemaField("volume", "INTEGER"),
    bigquery.SchemaField("daily_range", "FLOAT"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
]

news_schema = [
    bigquery.SchemaField("company", "STRING"),
    bigquery.SchemaField("ticker", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("published_at", "TIMESTAMP"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
]

trends_schema = [
    bigquery.SchemaField("position", "INTEGER"),
    bigquery.SchemaField("query", "STRING"),
    bigquery.SchemaField("search_volume", "FLOAT"),
    bigquery.SchemaField("percentage_increase", "INTEGER"),
    bigquery.SchemaField("location", "STRING"),
    bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
    bigquery.SchemaField("start_date", "TIMESTAMP"),
    bigquery.SchemaField("end_date", "TIMESTAMP"),
    bigquery.SchemaField("time_active_minutes", "FLOAT"),
    bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
]

schema_map = {
    "stocks": stocks_schema,
    "news": news_schema,
    "trends": trends_schema,
}
