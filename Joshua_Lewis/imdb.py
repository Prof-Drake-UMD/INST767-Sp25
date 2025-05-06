import requests
from datetime import datetime

def get_popular_movies(api_key):
    """
    Fetch popular movies using IMDb API
    
    Args:
        api_key (str): IMDb API key
    
    Returns:
        list: List of popular movies if successful, None if request fails
    """
    base_url = "https://api.imdb.com/API/MostPopularMovies"
    headers = {
        "X-RapidAPI-Key": api_key,
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        movies_data = response.json()
        
        movies = []
        for movie in movies_data["items"][:10]:  # Get top 10 movies
            movie_details = {
                "title": movie["title"],
                "year": movie["year"],
                "imdb_rating": movie["imDbRating"],
                "rank": movie["rank"]
            }
            movies.append(movie_details)
        return movies
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching movie data: {e}")
        return None

# Example usage:
# IMDB_API_KEY = "your_api_key_here"
# popular_movies = get_popular_movies(IMDB_API_KEY)
# if popular_movies:
#     for movie in popular_movies:
#         print(f"{movie['rank']}. {movie['title']} ({movie['year']}) - Rating: {movie['imdb_rating']}")


if __name__ == "__main__":
    # Example usage:
    IMDB_API_KEY = "your_api_key_here"
    popular_movies = get_popular_movies(IMDB_API_KEY)
    if popular_movies:
        for movie in popular_movies:
            print(f"{movie['rank']}. {movie['title']} ({movie['year']}) - Rating: {movie['imdb_rating']}")``