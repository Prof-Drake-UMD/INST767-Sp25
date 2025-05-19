CREATE TABLE IF NOT EXISTS mohammed_alazem_dataset.open_library_books (
    isbn13 STRING OPTIONS(description="ISBN-13 of the book, used for joining"),
    title STRING OPTIONS(description="Title of the book from Open Library"),
    subtitle STRING OPTIONS(description="Subtitle of the book, if available"),
    authors ARRAY<STRING> OPTIONS(description="List of author names"),
    publish_date STRING OPTIONS(description="Publication date string from Open Library"),
    publishers ARRAY<STRING> OPTIONS(description="List of publisher names"),
    publish_places ARRAY<STRING> OPTIONS(description="List of publication places"),
    number_of_pages INT64 OPTIONS(description="Number of pages, if available"),
    subjects ARRAY<STRING> OPTIONS(description="List of subjects or genres"),
    cover_url STRING OPTIONS(description="URL to large cover image, if available"),
    -- Core fields above, less common fields in JSON below
    additional_metadata JSON OPTIONS(description="Additional book metadata stored as JSON"),
    ingest_date DATE OPTIONS(description="Date when the record was ingested")
)
PARTITION BY ingest_date
OPTIONS(
    description="Table storing book metadata from Open Library with frequently used fields as columns and less common fields in JSON."
);