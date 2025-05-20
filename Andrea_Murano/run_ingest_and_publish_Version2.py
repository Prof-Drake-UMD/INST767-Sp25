import os
import json
from google.cloud import pubsub_v1
from transform_function.ingest.get_books import get_book_data
from transform_function.ingest.get_art import get_artworks_near_year
from transform_function.ingest.get_music import get_spotify_token, get_spotify_tracks_by_year

project_id = 'inst767-murano'
topic_id = 'ingest-to-transform'

def run_pipeline(book_title, author_name):
    book = get_book_data(book_title, author_name)
    if not book:
        print("No book found.")
        return

    year = book.get("first_publish_year")
    if not year:
        print("No publish year found.")
        return

    artworks = get_artworks_near_year(year)
    token = get_spotify_token()
    music = get_spotify_tracks_by_year(year, token)

    payload = {
        "book": book,
        "artworks": artworks,
        "music": music
    }

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    run_pipeline("The Great Gatsby", "F. Scott Fitzgerald")
