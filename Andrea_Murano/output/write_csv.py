import os
from datetime import datetime
import pandas as pd
from ingest_function.api_logic import match_cultural_experience_by_year

def main():
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"cultural_experience_{timestamp}.csv"
    output_path = os.path.join(output_dir, output_filename)

    book_title = "Normal People"
    author = "Sally Rooney"

    print(f"Generating cultural rows for book: {book_title} (Author: {author})")
    result = match_cultural_experience_by_year(book_title, author)
    if result and "book" in result:
        rows = []
        book = result["book"]
        for i in range(max(len(result["artworks"]), len(result["music"]), 1)):
            art = result["artworks"][i] if i < len(result["artworks"]) else {}
            mus = result["music"][i] if i < len(result["music"]) else {}
            row = {
                "book_id": book.get("book_id"),
                "book_title": book.get("title"),
                "book_author": book.get("author_name"),
                "book_first_publish_year": book.get("first_publish_year"),
                "book_language": book.get("language"),
                "book_url": book.get("book_url"),
                "object_id": art.get("object_id"),
                "artwork_title": art.get("title"),
                "artwork_artist": art.get("artist_name"),
                "artwork_medium": art.get("medium"),
                "artwork_date": art.get("object_date"),
                "artwork_url": art.get("object_url"),
                "artwork_image_url": art.get("image_url"),
                "track_id": mus.get("track_id"),
                "track_title": mus.get("title"),
                "track_artist": mus.get("artist"),
                "album_title": mus.get("album"),
                "track_release_date": mus.get("release_date"),
                "track_preview_url": mus.get("preview_url"),
                "match_type": result.get("step"),
                "ingest_ts": book.get("ingest_ts")
            }
            rows.append(row)
        columns = [
            "book_id", "book_title", "book_author", "book_first_publish_year", "book_language", "book_url",
            "object_id", "artwork_title", "artwork_artist", "artwork_medium", "artwork_date", "artwork_url", "artwork_image_url",
            "track_id", "track_title", "track_artist", "album_title", "track_release_date", "track_preview_url",
            "match_type", "ingest_ts"
        ]
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(output_path, index=False)
        print(f"CSV saved to {output_path}")
    else:
        print("No data generated.")

if __name__ == "__main__":
    main()
