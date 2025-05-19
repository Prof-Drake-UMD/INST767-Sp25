from ingest.get_books import get_book_data
from ingest.get_art import get_artworks_near_year
from ingest.get_music import get_spotify_tracks_by_year, get_spotify_token
from datetime import datetime

def build_cultural_rows(book_title, author=None, art_buffer=10, limit=5, max_rows=50):
    if not book_title:
        raise ValueError("Book title is required.")
    try:
        book = get_book_data(book_title, author)
        if not book or not book.get("first_publish_year"):
            print("Book or publication year not found.")
            return []
        year = book["first_publish_year"]
        artworks = get_artworks_near_year(year, buffer=art_buffer, max_results=limit)
        token = get_spotify_token()
        music = get_spotify_tracks_by_year(year, token=token, limit=limit)
        if not artworks or not music:
            print(f"Warning: No art or music found for year {year}.")
        rows = []
        for art in artworks:
            for track in music:
                if len(rows) >= max_rows:
                    break
                rows.append({
                    "book_title": book.get("title", ""),
                    "book_author": book.get("author_name", ""),
                    "book_first_publish_year": year,
                    "book_url": book.get("book_url", ""),
                    "language": book.get("language", ""),
                    "artwork_title": art.get("title", ""),
                    "artwork_artist": art.get("artist_name", ""),
                    "artwork_medium": art.get("medium", ""),
                    "artwork_date": art.get("object_date", ""),
                    "artwork_url": art.get("object_url", ""),
                    "track_title": track.get("title", ""),
                    "track_artist": track.get("artist", ""),
                    "album_title": track.get("album", ""),
                    "track_release_date": track.get("release_date", ""),
                    "preview_url": track.get("preview_url", ""),
                    "ingest_ts": datetime.utcnow().isoformat()
                })
        print(f"Built {len(rows)} cultural rows.")
        return rows
    except Exception as e:
        print(f"Error building rows: {e}")
        return []
