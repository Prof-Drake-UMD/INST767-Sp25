import requests
from datetime import datetime

def get_book_data(title, author=None):
    if not title:
        raise ValueError("Title is required to fetch book data")
    query = title if not author else f"{title} {author}"
    url = f"https://openlibrary.org/search.json?q={query}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"OpenLibrary API error: {resp.status_code}")
            return None
        docs = resp.json().get("docs", [])
        for book in docs:
            authors = [a.lower() for a in book.get("author_name", [])]
            if "first_publish_year" in book and (not author or author.lower() in authors):
                return {
                    "book_id": book.get("key"),
                    "title": book.get("title"),
                    "author_name": book.get("author_name", [None])[0],
                    "first_publish_year": book.get("first_publish_year"),
                    "language": book.get("language", [None])[0],
                    "book_url": f"https://openlibrary.org{book.get('key')}",
                    "ingest_ts": datetime.utcnow().isoformat()
                }
    except Exception as e:
        print(f"Book API exception: {e}")
    return None
