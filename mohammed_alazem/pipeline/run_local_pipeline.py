# run_pipeline.py

from book_pipeline_consumer_func.api_ingest import nyt_books, open_library, google_books
from book_pipeline_consumer_func.subscribers.process_nyt_subscriber import NytBookTransformer
from book_pipeline_consumer_func.subscribers.process_open_library_subscriber import OpenLibraryBookTransformer
from book_pipeline_consumer_func.subscribers.process_google_books_subscriber import GoogleBooksTransformer
import logging
import pandas as pd
import os 

logger = logging.getLogger("run_pipeline")
logging.basicConfig(level=logging.INFO)

LOCAL_NYT_API_KEY = os.environ.get("NYT_API_KEY")
LOCAL_GOOGLE_BOOKS_API_KEY = os.environ.get("GOOGLE_BOOKS_API_KEY")

def run_full_pipeline():
    logger.info("📚 Running full book data pipeline locally...")

    if not LOCAL_NYT_API_KEY:
        logger.error("NYT_API_KEY environment variable not set. Cannot fetch NYT data locally.")
        return
    if not LOCAL_GOOGLE_BOOKS_API_KEY:
        logger.error("GOOGLE_BOOKS_API_KEY environment variable not set. Cannot fetch Google Books data locally.")
        return 

    # Step 1: NYT Books
    logger.info(f"Fetching NYT data locally using provided API key...")
    nyt_response_data = nyt_books.fetch_nyt_bestsellers(api_key=LOCAL_NYT_API_KEY, list_name_encoded="hardcover-fiction")
    
    nyt_book_list = []
    if nyt_response_data and "results" in nyt_response_data and "books" in nyt_response_data["results"]:
        nyt_book_list = nyt_response_data["results"]["books"]
        logger.info(f"Fetched {len(nyt_book_list)} NYT books from the list '{nyt_response_data.get('results', {}).get('display_name', 'N/A')}'.")
    else:
        logger.info("NYT API response did not contain a 'books' list or was empty/invalid.")
        if nyt_response_data:
            logger.info(f"NYT API Response keys: {list(nyt_response_data.keys())}")

    # Transform NYT data
    if nyt_book_list:
        nyt_transformer = NytBookTransformer(event_id="local_pipeline")
        transformed_nyt_books, skipped_nyt = nyt_transformer.transform_data(nyt_book_list)
        if transformed_nyt_books:
            df_nyt = pd.DataFrame(transformed_nyt_books)
            df_nyt.to_csv("nyt_books.csv", index=False)
            logger.info(f"Transformed and saved {len(transformed_nyt_books)} NYT books to nyt_books.csv (skipped {skipped_nyt})")
        else:
            logger.info("No NYT books to save after transformation.")
    else:
        logger.info("No data fetched from NYT to transform and save.")

    # Step 2: Extract ISBNs
    isbn_list = []
    if nyt_book_list:
        isbn_list = [
            book.get("primary_isbn13", "").replace("-", "")
            for book in nyt_book_list
            if book.get("primary_isbn13")
        ]
        isbn10_list = [
            book.get("primary_isbn10", "").replace("-", "")
            for book in nyt_book_list
            if book.get("primary_isbn10")
        ]
        combined_isbns = list(set([isbn for isbn in isbn_list + isbn10_list if isbn]))
        logger.info(f"Extracted {len(combined_isbns)} unique ISBNs (13 and 10) for further processing: {combined_isbns[:10]}...")
        isbn_list = combined_isbns
    else:
        logger.info("Skipping ISBN extraction as NYT book list is empty.")

    # Step 3: Open Library
    openlib_data = []
    if isbn_list:
        logger.info(f"Fetching Open Library data locally for {len(isbn_list)} ISBNs...")
        openlib_data = open_library.fetch_open_library_books(isbn_list=isbn_list)
        logger.info(f"Fetched {len(openlib_data)} OpenLibrary records.")
        
        # Transform Open Library data
        if openlib_data:
            openlib_transformer = OpenLibraryBookTransformer(event_id="local_pipeline")
            transformed_openlib_books, skipped_openlib = openlib_transformer.transform_data(openlib_data)
            if transformed_openlib_books:
                df_openlib = pd.DataFrame(transformed_openlib_books)
                df_openlib.to_csv("open_library_books.csv", index=False)
                logger.info(f"Transformed and saved {len(transformed_openlib_books)} Open Library books to open_library_books.csv (skipped {skipped_openlib})")
            else:
                logger.info("No Open Library books to save after transformation.")
        else:
            logger.info("No data fetched from Open Library to transform and save.")
    else:
        logger.info("Skipping Open Library fetch as ISBN list is empty.")

    # Step 4: Google Books API
    google_books_data = []
    if isbn_list:
        logger.info(f"Fetching Google Books data locally for {len(isbn_list)} ISBNs using provided API key...")
        google_books_data = google_books.fetch_google_books_data(api_key=LOCAL_GOOGLE_BOOKS_API_KEY, isbn_list=isbn_list)
        logger.info(f"Fetched {len(google_books_data)} Google Books entries.")
        
        # Transform Google Books data
        if google_books_data:
            google_books_transformer = GoogleBooksTransformer(event_id="local_pipeline")
            transformed_google_books, skipped_google = google_books_transformer.transform_data(google_books_data)
            if transformed_google_books:
                df_google_books = pd.DataFrame(transformed_google_books)
                df_google_books.to_csv("google_books.csv", index=False)
                logger.info(f"Transformed and saved {len(transformed_google_books)} Google Books to google_books.csv (skipped {skipped_google})")
            else:
                logger.info("No Google Books to save after transformation.")
        else:
            logger.info("No data fetched from Google Books to transform and save.")
    else:
        logger.info("Skipping Google Books fetch as ISBN list is empty.")

    logger.info("✅ Local pipeline run complete. CSV files generated.")

if __name__ == "__main__":
    run_full_pipeline()