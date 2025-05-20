CREATE DATASET IF NOT EXISTS cultural_data;

CREATE TABLE IF NOT EXISTS cultural_data.books (
    ingest_ts TIMESTAMP,
    book_id STRING,
    title STRING,
    author_name STRING,
    first_publish_year INT64,
    language STRING,
    book_url STRING
);

CREATE TABLE IF NOT EXISTS cultural_data.artworks (
    ingest_ts TIMESTAMP,
    book_id STRING,
    object_id STRING,
    title STRING,
    artist_name STRING,
    medium STRING,
    object_date STRING,
    object_url STRING,
    image_url STRING
);

CREATE TABLE IF NOT EXISTS cultural_data.music_tracks (
    ingest_ts TIMESTAMP,
    book_id STRING,
    track_id STRING,
    title STRING,
    artist STRING,
    album STRING,
    release_date STRING,
    preview_url STRING
);
