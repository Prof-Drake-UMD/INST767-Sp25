CREATE TABLE IF NOT EXISTS mohammed_alazem_dataset.nyt_books (
    isbn13 STRING OPTIONS(description="Primary ISBN-13 of the book, used for joining"),
    title STRING OPTIONS(description="Title of the book"),
    author STRING OPTIONS(description="Author of the book"),
    publisher STRING OPTIONS(description="Publisher of the book"),
    description STRING OPTIONS(description="Book description from NYT"),
    rank INT64 OPTIONS(description="Current rank on the bestseller list"),
    rank_last_week INT64 OPTIONS(description="Rank on the bestseller list last week"),
    weeks_on_list INT64 OPTIONS(description="Number of weeks on the bestseller list"),
    isbn10 STRING OPTIONS(description="Primary ISBN-10 of the book"),
    amazon_product_url STRING OPTIONS(description="URL to the book on Amazon"),
    bestseller_date DATE OPTIONS(description="The publication date of the bestseller list"),
    list_name STRING OPTIONS(description="The encoded name of the bestseller list (e.g., hardcover-fiction)"),
    ingest_date DATE OPTIONS(description="Date when the record was ingested")
)
PARTITION BY ingest_date
OPTIONS(
    description="Table storing New York Times bestseller book information."
); 