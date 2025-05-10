import json
import os
import csv
from pull_now_playing_movies import gather_movie_full_data
from pull_top_rated_movies import gather_top_rated_movies
from clean_transform_movies import clean_movie

def save_to_csv(cleaned_movies: list, filename: str):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if not cleaned_movies:
        print(f"No movies to save for {filename}")
        return
    
    fieldnames = cleaned_movies[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for movie in cleaned_movies:
            writer.writerow(movie)
    
    print(f"✅ Saved {len(cleaned_movies)} movies to {filename}.")

def load_or_fetch_now_playing():
    json_path = "../now_playing_movies.json"
    if os.path.exists(json_path):
        print("Found existing now_playing_movies.json. Loading...")
        with open(json_path, "r", encoding="utf-8") as f:
            movies = json.load(f)
    else:
        print("No local file found. Fetching from API...")
        movies = gather_movie_full_data(region='US')
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(movies, f, indent=2)
    return movies

def load_or_fetch_top_rated():
    json_path = "../top_rated_movies.json"
    if os.path.exists(json_path):
        print("Found existing top_rated_movies.json. Loading...")
        with open(json_path, "r", encoding="utf-8") as f:
            movies = json.load(f)
    else:
        print("No local file found. Fetching from API...")
        movies = gather_top_rated_movies(region='US')
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(movies, f, indent=2)
    return movies

def main():
    # Now Playing
    now_playing_raw = load_or_fetch_now_playing()
    now_playing_cleaned = [clean_movie(m, is_now_playing=True) for m in now_playing_raw]
    save_to_csv(now_playing_cleaned, "../data/now_playing_movies.csv")

    # Top Rated
    top_rated_raw = load_or_fetch_top_rated()
    top_rated_cleaned = [clean_movie(m, is_now_playing=False) for m in top_rated_raw]
    save_to_csv(top_rated_cleaned, "../data/top_rated_movies.csv")
    print("✅ All movies processed and saved.")

if __name__ == "__main__":
    main()
