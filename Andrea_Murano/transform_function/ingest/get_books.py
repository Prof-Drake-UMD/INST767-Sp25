import requests
from datetime import datetime

def get_book_data(title, author=None):
    if not title:
        raise ValueError("Title is required to fetch book data")
    query = f"{title} {author}".strip() if author else title
    url = f"https://openlibrary.org/search.json?q={query}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"OpenLibrary API error: {resp.status_code}")
            return None
        docs = resp.json().get("docs", [])
        for book in docs[:15]:
            authors = [a.lower() for a in book.get("author_name", [])]
            publish_year = book.get("first_publish_year")
            work_key = book.get("key")
            languages = book.get("language", [])
            if (
                (not author or author.lower() in authors) and
                "eng" in languages and
                publish_year and publish_year >= 1950 and
                work_key
            ):
                return {
                    "book_id": work_key,
                    "title": book.get("title"),
                    "author_name": book.get("author_name", [None])[0],
                    "first_publish_year": publish_year,
                    "language": "eng",
                    "book_url": f"https://openlibrary.org{work_key}",
                    "ingest_ts": datetime.utcnow().isoformat()
                }
    except Exception as e:
        print(f"Book API exception: {e}")
    return None
