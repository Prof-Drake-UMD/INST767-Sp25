import json
import requests
import os
from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()
TMDB_API_KEY = os.environ.get('TMDB_API_KEY')
_genre_cache = None  # local cache

def fetch_genre_mapping():
    global _genre_cache
    if _genre_cache is not None:
        return _genre_cache

    url = "https://api.themoviedb.org/3/genre/movie/list"
    params = {
        'api_key': TMDB_API_KEY,
        'language': 'en-US'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    genres = response.json().get('genres', [])
    _genre_cache = {genre['id']: genre['name'] for genre in genres}
    return _genre_cache

def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def clean_movie(movie: dict, is_now_playing=True) -> dict:
    genre_ids = movie.get("genre_ids") or movie.get("genres") or []
    genres_mapped = []

    genre_mapping = fetch_genre_mapping()

    if isinstance(genre_ids, list):
        for genre_id in genre_ids:
            genre_name = genre_mapping.get(genre_id)
            if genre_name:
                genres_mapped.append(genre_name)

    cleaned = {
        "movie_id": str(movie.get("tmdb_id") or movie.get("movie_id") or ""),
        "title": movie.get("title", ""),
        "release_date": movie.get("release_date", None),
        "popularity": safe_float(movie.get("popularity")),
        "vote_average": safe_float(movie.get("vote_average")),
        "vote_count": movie.get("vote_count"),
        "genres": genres_mapped,
        "overview": movie.get("overview", ""),
        "box_office": movie.get("box_office", ""),
        "imdb_rating": safe_float(movie.get("imdb_rating")),
        "rotten_tomatoes_rating": movie.get("rotten_tomatoes_rating", ""),
        "metacritic_rating": movie.get("metacritic_rating", ""),
        "awards": movie.get("awards", "")
    }

    if is_now_playing:
        sources = movie.get("watchmode_sources", [])
        cleaned["streaming_sources"] = [
            {
                "source_id": s.get("id", ""),
                "source_name": s.get("name", ""),
                "type": s.get("type", ""),
                "region": s.get("region", ""),
                "web_url": s.get("web_url", "")
            }
            for s in sources
        ] if isinstance(sources, list) else []
        cleaned["has_streaming"] = bool(movie.get("has_streaming", False))

    return cleaned

def clean_movies_list(movie_list: list, is_now_playing=True) -> list:
    return [clean_movie(movie, is_now_playing=is_now_playing) for movie in movie_list]

def load_json_to_bigquery(bucket_name, blob_path, dataset_id, table_id):
    client = bigquery.Client()
    uri = f"gs://{bucket_name}/{blob_path}"

    from schema import NOW_PLAYING_SCHEMA

    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=NOW_PLAYING_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

    print(f"📅 Starting BigQuery load job: {load_job.job_id}")
    load_job.result()
    print("✅ Data loaded into BigQuery table.")
