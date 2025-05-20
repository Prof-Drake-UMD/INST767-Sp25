-- 1. Find all art and music from the same year as a selected book
WITH book_info AS (
  SELECT * FROM `inst767-murano-cultural-lens.cultural_lens.books`
  WHERE title = 'Normal People'
)
SELECT
  b.title AS book_title,
  b.author_name,
  b.first_publish_year,
  a.title AS artwork_title,
  a.artist_name,
  a.medium,
  a.object_date,
  m.title AS track_title,
  m.artist,
  m.album,
  m.release_date
FROM book_info b
LEFT JOIN `inst767-murano-cultural-lens.cultural_lens.artworks` a
  ON SAFE_CAST(a.object_date AS INT64) = b.first_publish_year
LEFT JOIN `inst767-murano-cultural-lens.cultural_lens.music` m
  ON EXTRACT(YEAR FROM m.release_date) = b.first_publish_year
ORDER BY a.title, m.title;

-- 2. Top artists and authors for a given year
SELECT
  b.author_name AS book_author,
  a.artist_name AS artwork_artist,
  m.artist AS music_artist,
  COUNT(*) AS count
FROM `inst767-murano-cultural-lens.cultural_lens.books` b
LEFT JOIN `inst767-murano-cultural-lens.cultural_lens.artworks` a
  ON SAFE_CAST(a.object_date AS INT64) = b.first_publish_year
LEFT JOIN `inst767-murano-cultural-lens.cultural_lens.music` m
  ON EXTRACT(YEAR FROM m.release_date) = b.first_publish_year
WHERE b.first_publish_year = 2018
GROUP BY book_author, artwork_artist, music_artist
ORDER BY count DESC
LIMIT 10;

-- 3. List all works (books, art, music) from a given year
SELECT 'Book' AS type, title, author_name AS creator, first_publish_year AS year
FROM `inst767-murano-cultural-lens.cultural_lens.books`
WHERE first_publish_year = 2018
UNION ALL
SELECT 'Artwork', title, artist_name, SAFE_CAST(object_date AS INT64)
FROM `inst767-murano-cultural-lens.cultural_lens.artworks`
WHERE SAFE_CAST(object_date AS INT64) = 2018
UNION ALL
SELECT 'Music', title, artist, EXTRACT(YEAR FROM release_date)
FROM `inst767-murano-cultural-lens.cultural_lens.music`
WHERE EXTRACT(YEAR FROM release_date) = 2018
ORDER BY year, type, title;