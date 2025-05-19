import requests
import time
import json
import os
from dotenv import load_dotenv

# Load API keys from .env file
load_dotenv()

TMDB_API_KEY = os.getenv('TMDB_API_KEY')
OMDB_API_KEY = os.getenv('OMDB_API_KEY')

def fetch_tmdb_top_rated(language='en-US', page=1, region=None):
    url = "https://api.themoviedb.org/3/movie/top_rated"
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

def gather_top_rated_movies(region=None):
    movies_full_data = []

    # Fetch first page to get total pages
    first_page = fetch_tmdb_top_rated(region=region, page=1)
    total_pages = min(first_page.get('total_pages', 1), 5)  # 只拉5页，约100部

    print(f"Total pages to fetch (Top Rated): {total_pages}")

    # Handle first page
    all_movies = first_page.get('results', [])

    # Keep fetching until all pages are processed
    for page in range(2, total_pages + 1):
        try:
            page_data = fetch_tmdb_top_rated(region=region, page=page)
            page_movies = page_data.get('results', [])
            all_movies.extend(page_movies)
            time.sleep(2)
        except Exception as e:
            print(f"Failed to fetch page {page}: {str(e)}")
            continue

    print(f"Total top rated movies fetched: {len(all_movies)}")

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

            if imdb_id:
                omdb_data = fetch_omdb_movie_data(imdb_id)
                time.sleep(1)

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
                'awards': omdb_data.get('Awards')
            })

            time.sleep(2)

        except Exception as e:
            print(f"Failed to process movie {movie.get('title')}: {str(e)}")
            continue

    return movies_full_data

if __name__ == "__main__":
    movies_data = gather_top_rated_movies(region='US')
    print(f"Fetched and combined data for {len(movies_data)} top rated movies.")

    # Save to JSON
    with open('top_rated_movies.json', 'w', encoding='utf-8') as f:
        json.dump(movies_data, f, indent=2)
    print("Data saved to top_rated_movies.json!")
