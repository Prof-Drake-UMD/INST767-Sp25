from google.cloud import bigquery
# Define schema for now_playing_movies
NOW_PLAYING_SCHEMA = [
    bigquery.SchemaField("movie_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("release_date", "DATE"),
    bigquery.SchemaField("popularity", "FLOAT64"),
    bigquery.SchemaField("vote_average", "FLOAT64"),
    bigquery.SchemaField("vote_count", "INT64"),
    bigquery.SchemaField("genres", "STRING", mode="REPEATED"),
    bigquery.SchemaField("overview", "STRING"),
    bigquery.SchemaField("box_office", "STRING"),
    bigquery.SchemaField("imdb_rating", "FLOAT64"),
    bigquery.SchemaField("rotten_tomatoes_rating", "STRING"),
    bigquery.SchemaField("metacritic_rating", "STRING"),
    bigquery.SchemaField("awards", "STRING"),
    bigquery.SchemaField("streaming_sources", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("source_id", "INT64"),
        bigquery.SchemaField("source_name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("web_url", "STRING")
    ]),
    bigquery.SchemaField("has_streaming", "BOOL"),
]

# Define schema for top_rated_movies
TOP_RATED_SCHEMA = [
    bigquery.SchemaField("movie_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("release_date", "DATE"),
    bigquery.SchemaField("popularity", "FLOAT64"),
    bigquery.SchemaField("vote_average", "FLOAT64"),
    bigquery.SchemaField("vote_count", "INT64"),
    bigquery.SchemaField("genres", "STRING", mode="REPEATED"),
    bigquery.SchemaField("overview", "STRING"),
    bigquery.SchemaField("box_office", "STRING"),
    bigquery.SchemaField("imdb_rating", "FLOAT64"),
    bigquery.SchemaField("rotten_tomatoes_rating", "STRING"),
    bigquery.SchemaField("metacritic_rating", "STRING"),
    bigquery.SchemaField("awards", "STRING"),
]