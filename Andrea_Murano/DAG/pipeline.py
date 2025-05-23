import os
from google.cloud import bigquery
from DAG.api_calls import fetch_books, fetch_artwork, get_spotify_token, fetch_music

PROJECT_ID = "inst767-murano-cultural-lens"
DATASET = "cultural_lens"

def write_to_bigquery(table, rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print(f"BigQuery error for {table}:", errors)
    else:
        print(f"Wrote {len(rows)} rows to {table}")

def main():
    books = fetch_books()
    if books:
        write_to_bigquery("books", books)
        year = books[0]['first_publish_year']
    else:
        print("No books found.")
        return

    artwork = fetch_artwork()
    if artwork:
        write_to_bigquery("artwork", artwork)

    spotify_id = os.environ.get("SPOTIFY_CLIENT_ID")
    spotify_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    if not spotify_id or not spotify_secret:
        print("Spotify credentials not set.")
        return
    token = get_spotify_token(spotify_id, spotify_secret)
    music = fetch_music(token)
    if music:
        write_to_bigquery("music", music)

if __name__ == "__main__":
    main()
