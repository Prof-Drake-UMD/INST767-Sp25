import json
import requests
import os
from dotenv import load_dotenv

# Load API keys
load_dotenv()
TMDB_API_KEY = os.getenv('TMDB_API_KEY')

# ===== Step 1: Fetch genre id -> name mapping =====
def fetch_genre_mapping():
    url = "https://api.themoviedb.org/3/genre/movie/list"
    params = {
        'api_key': TMDB_API_KEY,
        'language': 'en-US'
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    genres = response.json().get('genres', [])
    
    # Create a dictionary {id: name}
    genre_mapping = {genre['id']: genre['name'] for genre in genres}
    return genre_mapping

# Pull genre mapping once
GENRE_MAPPING = fetch_genre_mapping()

# ===== Step 2: Helper function =====
def safe_float(value):
    """Helper to safely convert a value to float, or return None."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

# ===== Step 3: Main clean_movie function =====
def clean_movie(movie: dict, is_now_playing=True) -> dict:
    """Clean and transform a single movie dictionary."""

    # Map genre_ids to genre names
    genre_ids = movie.get("genre_ids") or movie.get("genres") or []
    genres_mapped = []
    if isinstance(genre_ids, list):
        for genre_id in genre_ids:
            genre_name = GENRE_MAPPING.get(genre_id)
            if genre_name:
                genres_mapped.append(genre_name)

    # Build cleaned movie
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
        # If Now Playing movies, add streaming fields
        streaming_sources = movie.get("watchmode_sources", [])
        if streaming_sources and isinstance(streaming_sources, list):
            sources_cleaned = []
            for source in streaming_sources:
                sources_cleaned.append({
                    "source_name": source.get("name", ""),
                    "type": source.get("type", ""),
                    "region": source.get("region", ""),
                    "url": source.get("web_url", "")
                })
            cleaned["streaming_sources"] = sources_cleaned
        else:
            cleaned["streaming_sources"] = []
        
        cleaned["has_streaming"] = bool(movie.get("has_streaming", False))

    return cleaned
