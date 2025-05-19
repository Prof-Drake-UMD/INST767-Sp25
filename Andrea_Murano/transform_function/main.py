import base64
import json
from ingest.get_books import get_book_data
from ingest.get_art import get_artworks_near_year
from ingest.get_music import get_spotify_token, get_spotify_tracks_by_year

def transform(event, context):
    """Background Cloud Function to be triggered by Pub/Sub."""
    # Decode Pub/Sub message data
    if 'data' in event:
        try:
            data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        except Exception as e:
            print(f"Error decoding Pub/Sub data: {e}")
            return

        book_title = data.get('book_title')
        author_name = data.get('author_name')
        if not book_title:
            print("Missing book_title in Pub/Sub message")
            return

        # Get book metadata
        book = get_book_data(book_title, author_name)
        if not book or not book.get("first_publish_year"):
            print(f"No book data found for '{book_title}' by '{author_name}'")
            return

        # Get artwork
        try:
            artworks = get_artworks_near_year(book["first_publish_year"])
        except Exception as e:
            print(f"Error fetching artworks: {e}")
            artworks = []

        # Get Spotify music tracks
        try:
            token = get_spotify_token()
            music = get_spotify_tracks_by_year(book["first_publish_year"], token)
        except Exception as e:
            print(f"Error fetching Spotify tracks: {e}")
            music = []

        # Compose result
        result = {
            "book": book,
            "artworks": artworks,
            "music": music
        }
        print(json.dumps(result, ensure_ascii=False))
    else:
        print("No data found in event")
