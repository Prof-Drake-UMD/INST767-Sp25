# -*- coding: utf-8 -*-

import requests
import time
import json
from dotenv import load_dotenv
import os

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
WATCHMODE_API_KEY = os.getenv("WATCHMODE_API_KEY")
def fetch_tmdb_now_playing(language='en-US', page=1, region=None):
    url = "https://api.themoviedb.org/3/movie/now_playing"
    params = {
        'api_key': TMDB_API_KEY,
        'language': language,
        'page': page
    }
    if region:
        params['region'] = region

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_tmdb_external_ids(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/external_ids"
    params = {
        'api_key': TMDB_API_KEY
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_omdb_movie_data(imdb_id):
    url = "http://www.omdbapi.com/"
    params = {
        'i': imdb_id,
        'apikey': OMDB_API_KEY
    }
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
    response.raise_for_status()
    data = response.json()
    if data.get('title_results'):
        return data['title_results'][0]['id']
    else:
        return None

def fetch_watchmode_sources(title_id):
    url = f"https://api.watchmode.com/v1/title/{title_id}/sources/"
    params = {
        'apiKey': WATCHMODE_API_KEY
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def gather_movie_full_data(region=None):
    movies_full_data = []

    # get the first page to find out total pages
    first_page = fetch_tmdb_now_playing(region=region, page=1)
    total_pages = first_page.get('total_pages', 1)

    print(f"Total pages to fetch: {total_pages}")

    # handle the first page
    all_movies = first_page.get('results', [])

    # keep fetching until all pages are processed
    for page in range(2, total_pages + 1):
        try:
            page_data = fetch_tmdb_now_playing(region=region, page=page)
            page_movies = page_data.get('results', [])
            all_movies.extend(page_movies)
            time.sleep(0.2)  # to avoid hitting the API rate limit
        except Exception as e:
            print(f"Failed to fetch page {page}: {str(e)}")
            continue

    print(f"Total movies fetched from TMDB: {len(all_movies)}")

    for movie in all_movies:
        try:
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
                omdb_data = fetch_omdb_movie_data(imdb_id)
                watchmode_title_id = fetch_watchmode_title_id(imdb_id)
                if watchmode_title_id:
                    try:
                        watchmode_sources = fetch_watchmode_sources(watchmode_title_id)
                        if watchmode_sources and isinstance(watchmode_sources, list):
                            has_streaming = len(watchmode_sources) > 0
                    except Exception as e:
                        print(f"Watchmode sources fetch failed for {title}: {str(e)}")
                        watchmode_sources = []
                        has_streaming = False

            movies_full_data.append({
                'tmdb_id': movie_id,
                'title': title,
                'popularity': popularity,
                'release_date': release_date,
                'genre_ids': genre_ids,
                'vote_average': vote_average,
                'vote_count': vote_count,
                'imdb_id': imdb_id,
                'omdb': omdb_data,
                'watchmode_sources': watchmode_sources,
                'has_streaming': has_streaming
            })

            time.sleep(0.2)

        except Exception as e:
            print(f"Failed to process movie {movie.get('title')}: {str(e)}")
            continue

    return movies_full_data

if __name__ == "__main__":
    movies_data = gather_movie_full_data(region='US')
    print(f"Fetched and combined data for {len(movies_data)} movies.")

    # Save to JSON
    with open('movies_data.json', 'w') as f:
        json.dump(movies_data, f, indent=2)
    print("Data saved to movies_data.json!")