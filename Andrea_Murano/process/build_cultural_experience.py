from ingest.get_books import get_book_data
from ingest.get_art import get_artworks_near_year
from ingest.get_music import get_spotify_tracks_by_year
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_TOKEN = os.getenv("SPOTIFY_TOKEN")

def build_cultural_rows(book_title, author=None, art_buffer=10, limit=5):
    book = get_book_data(book_title, author)
    if not book:
        print("Book not found.")
        return []

    year = book.get("first_publish_year")
    if not year:
        print("No publication year available.")
        return []

    artworks = get_artworks_near_year(year, buffer=art_buffer, max_results=limit)
    music = get_spotify_tracks_by_year(year, token=SPOTIFY_TOKEN, limit=limit)

    rows = []
    for art in artworks:
        for track in music:
            rows.append({
                "book_title": book["title"],
                "book_author": book["author_name"],
                "book_first_publish_year": book["first_publish_year"],
                "book_url": book["book_url"],
                "language": book["language"],

                "artwork_title": art["title"],
                "artwork_artist": art["artist_name"],
                "artwork_medium": art["medium"],
                "artwork_date": art["object_date"],
                "artwork_url": art["object_url"],

                "track_title": track["title"],
                "track_artist": track["artist"],
                "album_title": track["album"],
                "track_release_date": track["release_date"],
                "preview_url": track["preview_url"],

                "ingest_ts": datetime.utcnow().isoformat()
            })

    return rows
