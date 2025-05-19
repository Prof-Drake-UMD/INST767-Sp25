import base64
import json
from ingest.get_books import get_book_metadata
from ingest.get_art import get_artworks
from ingest.get_music import get_music_tracks

def transform(event, context):
    """Background Cloud Function to be triggered by Pub/Sub."""
    if 'data' in event:
        data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        book_title = data.get('book_title')
        author_name = data.get('author_name')
        if not book_title or not author_name:
            print("Missing book_title or author_name")
            return

        book = get_book_metadata(book_title, author_name)
        artworks = get_artworks(book.get('first_publish_year'))
        music = get_music_tracks(book.get('first_publish_year'))
        result = {
            "book": book,
            "artworks": artworks,
            "music": music
        }
        print(json.dumps(result))  # For now, just log; later, insert to BigQuery
    else:
        print("No data found in event")
