import requests
from datetime import datetime

def get_book_data(title, author=None):
    query = title if not author else f"{title} {author}"
    url = f"https://openlibrary.org/search.json?q={query}"
    resp = requests.get(url)
    docs = resp.json().get("docs", [])
    
    for book in docs:
        if "first_publish_year" in book and (not author or author.lower() in str(book.get("author_name", [])).lower()):
            return {
                "book_id": book.get("key"),
                "title": book.get("title"),
                "author_name": book.get("author_name", [None])[0],
                "first_publish_year": book.get("first_publish_year"),
                "language": book.get("language", [None])[0],
                "book_url": f"https://openlibrary.org{book.get('key')}",
                "ingest_ts": datetime.utcnow().isoformat()
            }
    return None
