CREATE TABLE `glass-marker-458019-q6.movies_dataset.now_playing_movies` (
  movie_id STRING,
  title STRING,
  release_date DATE,
  popularity FLOAT64,
  vote_average FLOAT64,
  vote_count INT64,
  genres ARRAY<STRING>,
  overview STRING,
  box_office STRING,
  imdb_rating FLOAT64,
  rotten_tomatoes_rating STRING,
  metacritic_rating STRING,
  awards STRING,
  streaming_sources ARRAY<STRUCT<
    source_id INT64,
    source_name STRING,
    type STRING,
    region STRING,
    ios_url STRING,
    android_url STRING,
    web_url STRING,
    format STRING,
    price FLOAT64,
    seasons INT64,
    episodes INT64
  >>,
  has_streaming BOOL
);
