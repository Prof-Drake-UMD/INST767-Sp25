import os
import logging

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None
    logging.getLogger(__name__).warning(
        "python-dotenv library not found. .env file will not be automatically loaded. "
        "Consider installing it with: pip install python-dotenv"
    )

if load_dotenv:
    found_dotenv = load_dotenv()
    if found_dotenv:
        logging.info(f"book_pipeline_consumer_func: .env file loaded successfully.")
    else:
        logging.info("book_pipeline_consumer_func: No .env file found by load_dotenv() or it was empty.")
else:
    logging.info("book_pipeline_consumer_func: python-dotenv not available, .env file not loaded.")