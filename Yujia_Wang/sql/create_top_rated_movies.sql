CREATE TABLE `glass-marker-458019-q6.movies_dataset.top_rated_movies` (
  movie_id STRING,
  title STRING,
  release_date DATE,
  popularity FLOAT64,
  vote_average FLOAT64,
  genres ARRAY<STRING>,
  overview STRING,
  box_office STRING,
  imdb_rating FLOAT64,
  rotten_tomatoes_rating STRING,
  metacritic_rating STRING,
  awards STRING,
  streaming_sources ARRAY<STRUCT<
    source_name STRING,
    type STRING,
    region STRING,
    url STRING
  >>
);
