# google_cloud/ingest/fetch.py

# -*- coding: utf-8 -*-
import requests
import time
import json
import os
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
WATCHMODE_API_KEY = os.getenv("WATCHMODE_API_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = f"movie-data-bucket-{PROJECT_ID}"

# ----------------- GCS UTILS -----------------

def load_last_success_from_gcs(bucket_name):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("metadata/last_success.json")
        if blob.exists():
            content = blob.download_as_text()
            print("📁 Loaded last_success.json from GCS.")
            return json.loads(content)
        else:
            print("⚠️ last_success.json not found in GCS.")
    except Exception as e:
        print(f"❌ Error loading last_success from GCS: {e}")
    return []

def save_last_success_to_gcs(bucket_name, movies_data):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("metadata/last_success.json")
        blob.upload_from_string(json.dumps(movies_data, indent=2), content_type="application/json")
        print("✅ last_success.json uploaded to GCS.")
    except Exception as e:
        print(f"❌ Failed to upload last_success.json to GCS: {e}")

# ----------------- FETCH HELPERS -----------------

def fetch_tmdb_now_playing(language='en-US', page=1, region=None):
    url = "https://api.themoviedb.org/3/movie/now_playing"
    params = {'api_key': TMDB_API_KEY, 'language': language, 'page': page}
    if region:
        params['region'] = region
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_tmdb_external_ids(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/external_ids"
    params = {'api_key': TMDB_API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_omdb_movie_data(imdb_id):
    url = "http://www.omdbapi.com/"
    params = {'i': imdb_id, 'apikey': OMDB_API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_watchmode_title_id(imdb_id):
    url = "https://api.watchmode.com/v1/search/"
    params = {
        'apiKey': WATCHMODE_API_KEY,
        'search_field': 'imdb_id',
        'search_value': imdb_id
    }
    response = requests.get(url, params=params)
    if response.status_code == 429:
        raise requests.exceptions.HTTPError("429 Too Many Requests", response=response)
    response.raise_for_status()
    data = response.json()
    if data.get('title_results'):
        return data['title_results'][0]['id']
    return None

def fetch_watchmode_sources(title_id):
    url = f"https://api.watchmode.com/v1/title/{title_id}/sources/"
    params = {'apiKey': WATCHMODE_API_KEY}
    response = requests.get(url, params=params)
    if response.status_code == 429:
        raise requests.exceptions.HTTPError("429 Too Many Requests", response=response)
    response.raise_for_status()
    return response.json()

# ----------------- MAIN FETCH FUNCTION -----------------

def gather_movie_full_data(bucket_name, region=None):
    movies_full_data = []
    watchmode_failure_count = 0
    watchmode_failure_limit = 3

    try:
        first_page = fetch_tmdb_now_playing(region=region, page=1)
        total_pages = first_page.get('total_pages', 1)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("🛑 TMDB API quota exceeded. Using backup data.")
            return load_last_success_from_gcs(bucket_name)
        raise

    print(f"📄 Total pages to fetch: {total_pages}")
    all_movies = first_page.get('results', [])

    for page in range(2, total_pages + 1):
        try:
            page_data = fetch_tmdb_now_playing(region=region, page=page)
            all_movies.extend(page_data.get('results', []))
            time.sleep(2)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("🛑 TMDB quota exceeded mid-fetch. Falling back.")
                return load_last_success_from_gcs(bucket_name)
            print(f"❌ Failed to fetch page {page}: {e}")
            continue

    print(f"🎬 Total movies fetched from TMDB: {len(all_movies)}")

    for movie in all_movies:
        try:
            if watchmode_failure_count >= watchmode_failure_limit:
                print("🛑 Too many Watchmode failures. Using backup from GCS.")
                return load_last_success_from_gcs(bucket_name)

            movie_id = movie.get('id')
            title = movie.get('title')
            popularity = movie.get('popularity')
            release_date = movie.get('release_date')
            genre_ids = movie.get('genre_ids')
            vote_average = movie.get('vote_average')
            vote_count = movie.get('vote_count')

            external_ids = fetch_tmdb_external_ids(movie_id)
            imdb_id = external_ids.get('imdb_id')
            omdb_data = {}
            watchmode_sources = []
            has_streaming = False

            if imdb_id:
                try:
                    omdb_data = fetch_omdb_movie_data(imdb_id)
                    time.sleep(0.5)
                    try:
                        watchmode_title_id = fetch_watchmode_title_id(imdb_id)
                        time.sleep(0.5)
                        if watchmode_title_id:
                            watchmode_sources = fetch_watchmode_sources(watchmode_title_id)
                            has_streaming = bool(watchmode_sources)
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:
                            watchmode_failure_count += 1
                            print(f"⚠️ Watchmode quota hit for {title} (#{watchmode_failure_count}/3). Sleeping 30s...")
                            time.sleep(30)
                            continue
                        raise
                except Exception as e:
                    print(f"⚠️ Watchmode error for {title}: {e}")
                    watchmode_failure_count += 1

            movies_full_data.append({
                'tmdb_id': movie_id,
                'title': title,
                'popularity': popularity,
                'release_date': release_date,
                'genre_ids': genre_ids,
                'vote_average': vote_average,
                'vote_count': vote_count,
                'imdb_id': imdb_id,
                'box_office': omdb_data.get('BoxOffice'),
                'runtime': omdb_data.get('Runtime'),
                'imdb_rating': omdb_data.get('imdbRating'),
                'rotten_tomatoes_rating': next((r['Value'] for r in omdb_data.get('Ratings', []) if r['Source'] == 'Rotten Tomatoes'), None),
                'metacritic_rating': next((r['Value'] for r in omdb_data.get('Ratings', []) if r['Source'] == 'Metacritic'), None),
                'awards': omdb_data.get('Awards'),
                'watchmode_sources': watchmode_sources,
                'has_streaming': has_streaming
            })

            time.sleep(2)

        except Exception as e:
            print(f"❌ Failed to process movie {movie.get('title')}: {e}")
            continue

    save_last_success_to_gcs(bucket_name, movies_full_data)
    return movies_full_data

# ----------------- Optional local test -----------------

if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/service-account-key.json"
    movies = gather_movie_full_data(BUCKET_NAME, region='US')
    with open('now_playing_movies.json', 'w') as f:
        json.dump(movies, f, indent=2)
    print("✅ Saved to now_playing_movies.json")
