-- interesting_queries.sql

-- 1. Compare genre distribution between Now Playing and Top Rated movies
SELECT
  movie_group,
  genre,
  COUNT(*) AS genre_count
FROM (
  SELECT
    'Now Playing' AS movie_group,
    genre
  FROM
    `glass-marker-458019-q6.movies_dataset.now_playing_movies`,
    UNNEST(genres) AS genre
  UNION ALL
  SELECT
    'Top Rated' AS movie_group,
    genre
  FROM
    `glass-marker-458019-q6.movies_dataset.top_rated_movies`,
    UNNEST(genres) AS genre
)
GROUP BY
  movie_group, genre
ORDER BY
  movie_group, genre_count DESC;

-- 2. Analyze the relationship between Box Office revenue and IMDb rating
SELECT
  title,
  imdb_rating,
  SAFE_CAST(REPLACE(REPLACE(box_office, '$', ''), ',', '') AS FLOAT64) AS box_office_value
FROM
  `glass-marker-458019-q6.movies_dataset.now_playing_movies`
WHERE
  imdb_rating IS NOT NULL
  AND box_office IS NOT NULL
  AND SAFE_CAST(REPLACE(REPLACE(box_office, '$', ''), ',', '') AS FLOAT64) > 0;

-- 3. Calculate the average number of streaming platforms for movies released in the past year
SELECT
  AVG(ARRAY_LENGTH(streaming_sources)) AS avg_platforms
FROM
  `glass-marker-458019-q6.movies_dataset.now_playing_movies`
WHERE
  release_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  AND ARRAY_LENGTH(streaming_sources) > 0;

-- 4. Find the genres that most frequently win awards
SELECT
  genre,
  COUNT(*) AS award_winning_movie_count
FROM
  `glass-marker-458019-q6.movies_dataset.top_rated_movies`,
  UNNEST(genres) AS genre
WHERE
  awards IS NOT NULL
GROUP BY
  genre
ORDER BY
  award_winning_movie_count DESC;

-- 5. Compare the average IMDb ratings of movies across different decades
SELECT
  CASE
    WHEN EXTRACT(YEAR FROM release_date) BETWEEN 1970 AND 1979 THEN '1970s'
    WHEN EXTRACT(YEAR FROM release_date) BETWEEN 1980 AND 1989 THEN '1980s'
    WHEN EXTRACT(YEAR FROM release_date) BETWEEN 1990 AND 1999 THEN '1990s'
    WHEN EXTRACT(YEAR FROM release_date) BETWEEN 2000 AND 2009 THEN '2000s'
    WHEN EXTRACT(YEAR FROM release_date) BETWEEN 2010 AND 2019 THEN '2010s'
    WHEN EXTRACT(YEAR FROM release_date) >= 2020 THEN '2020s'
    ELSE 'Other'
  END AS decade,
  AVG(imdb_rating) AS avg_imdb_rating
FROM
  `glass-marker-458019-q6.movies_dataset.top_rated_movies`
WHERE
  imdb_rating IS NOT NULL
GROUP BY
  decade
ORDER BY
  decade;
