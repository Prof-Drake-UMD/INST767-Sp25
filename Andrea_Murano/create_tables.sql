-- Books Table
CREATE TABLE `inst767-murano-cultural-lens.cultural_lens.books` (
  book_id STRING,
  title STRING,
  author_name STRING,
  first_publish_year INT64,
  language STRING,
  book_url STRING,
  ingest_ts TIMESTAMP
);

-- Artwork Table
CREATE TABLE `inst767-murano-cultural-lens.cultural_lens.artwork` (
  object_id INT64,
  title STRING,
  artist_name STRING,
  medium STRING,
  object_date STRING,
  object_url STRING,
  image_url STRING,
  ingest_ts TIMESTAMP
);

-- Music Table
CREATE TABLE `inst767-murano-cultural-lens.cultural_lens.music` (
  track_id STRING,
  title STRING,
  artist STRING,
  album STRING,
  release_date DATE,
  preview_url STRING,
  ingest_ts TIMESTAMP
);
