from google.cloud import bigquery
import os

def insert_to_bigquery(data):
    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    dataset_id = "cultural_data"

    bq_client = bigquery.Client(project=project_id)

    book = data.get("book", {})
    if book:
        books_table = f"{project_id}.{dataset_id}.books"
        book_row = {
            "book_id": book.get("book_id"),
            "title": book.get("title"),
            "author_name": book.get("author_name"),
            "first_publish_year": book.get("first_publish_year"),
            "language": book.get("language"),
            "ingest_ts": book.get("ingest_ts"),
            "book_url": book.get("book_url"),  
        }
        errors = bq_client.insert_rows_json(books_table, [book_row])
        if errors:
            print(f"BigQuery book insert errors: {errors}")

    artworks = data.get("artworks", [])
    if book and artworks:
        artworks_table = f"{project_id}.{dataset_id}.artworks"
        artwork_rows = []
        for art in artworks:
            artwork_rows.append({
                "ingest_ts": book.get("ingest_ts"),
                "book_id": book.get("book_id"),
                "object_id": art.get("object_id"),
                "title": art.get("title"),
                "artist_name": art.get("artist_name"),
                "medium": art.get("medium"),
                "object_date": art.get("object_date"),
                "object_url": art.get("object_url"),
                "image_url": art.get("image_url"),
            })
        errors = bq_client.insert_rows_json(artworks_table, artwork_rows)
        if errors:
            print(f"BigQuery artworks insert errors: {errors}")

    music = data.get("music", [])
    if book and music:
        music_table = f"{project_id}.{dataset_id}.music_tracks"
        music_rows = []
        for track in music:
            music_rows.append({
                "ingest_ts": book.get("ingest_ts"),
                "book_id": book.get("book_id"),
                "track_id": track.get("track_id"),
                "title": track.get("title"),
                "artist": track.get("artist"),
                "album": track.get("album"),
                "release_date": track.get("release_date"),
                "preview_url": track.get("preview_url"),
            })
        errors = bq_client.insert_rows_json(music_table, music_rows)
        if errors:
            print(f"BigQuery music insert errors: {errors}")
