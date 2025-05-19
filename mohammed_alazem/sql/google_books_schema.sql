CREATE TABLE IF NOT EXISTS mohammed_alazem_dataset.google_books_details (
    isbn13 STRING OPTIONS(description="ISBN-13 of the book, used for joining"),
    google_id STRING OPTIONS(description="Google Books unique ID for the volume"),
    title STRING OPTIONS(description="Title of the book from Google Books"),
    subtitle STRING OPTIONS(description="Subtitle of the book, if any"),
    authors ARRAY<STRING> OPTIONS(description="List of authors from Google Books"),
    publisher STRING OPTIONS(description="Publisher from Google Books"),
    published_date STRING OPTIONS(description="Publication date string from Google Books (e.g., YYYY-MM-DD or YYYY)"),
    description STRING OPTIONS(description="Book description from Google Books"),
    page_count INT64 OPTIONS(description="Number of pages according to Google Books"),
    categories ARRAY<STRING> OPTIONS(description="List of categories/genres from Google Books"),
    average_rating FLOAT64 OPTIONS(description="Average user rating on Google Books"),
    ratings_count INT64 OPTIONS(description="Number of user ratings on Google Books"),
    maturity_rating STRING OPTIONS(description="Maturity rating (e.g., NOT_MATURE)"),
    language STRING OPTIONS(description="Book language code (e.g., en)"),
    preview_link STRING OPTIONS(description="URL to the book preview on Google Books"),
    info_link STRING OPTIONS(description="URL to the book information page on Google Books"),
    thumbnail_link STRING OPTIONS(description="URL to the book cover thumbnail image"),
    small_thumbnail_link STRING OPTIONS(description="URL to a smaller book cover thumbnail image"),
    saleability STRING OPTIONS(description="Saleability status (e.g., FOR_SALE, NOT_FOR_SALE)"),
    sale_country STRING OPTIONS(description="Country code for sale information (e.g., US, AE)"),
    list_price_amount FLOAT64 OPTIONS(description="List price amount, if available"),
    list_price_currency_code STRING OPTIONS(description="Currency code for list price (e.g., USD, EUR)"),
    buy_link STRING OPTIONS(description="URL to purchase the book, if available"),
    sale_info_json JSON OPTIONS(description="Original SaleInfo object from Google Books API stored as JSON"),
    access_info JSON OPTIONS(description="Access information from Google Books API stored as JSON"),
    additional_metadata JSON OPTIONS(description="Additional book metadata from Google Books API stored as JSON"),
    ingest_date DATE OPTIONS(description="Date when the record was ingested")
)
PARTITION BY ingest_date
OPTIONS(
    description="Table storing detailed book information from Google Books API."
); 