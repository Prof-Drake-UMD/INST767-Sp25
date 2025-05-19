# main.py
# This file acts as the entry point for Google Cloud Functions
# when deploying from the project root. It re-exports the actual
# function handlers from their respective modules within the package.

# --- Ingest Functions ---
from book_pipeline_consumer_func.api_ingest.nyt_books import nyt_http_and_pubsub_trigger
from book_pipeline_consumer_func.api_ingest.open_library import open_library_http_and_pubsub_trigger
from book_pipeline_consumer_func.api_ingest.google_books import google_books_http_and_pubsub_trigger

# --- Subscriber Functions ---
from book_pipeline_consumer_func.subscribers.process_nyt_subscriber import process_nyt_data_subscriber
from book_pipeline_consumer_func.subscribers.process_open_library_subscriber import process_open_library_data_subscriber
from book_pipeline_consumer_func.subscribers.process_google_books_subscriber import process_google_books_data_subscriber

# The gcloud deploy command will reference these re-exported functions by their names
# as defined in this main.py, e.g., --entry-point nyt_http_and_pubsub_trigger