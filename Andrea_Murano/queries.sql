------------------------------ 1. Most Highly Active Combinations of Creators in a Given Year ------------------------------
-- Find combinations (band + author + artist) with the most works released in a specific year.
SELECT
  COALESCE(b.author_name, 'N/A') AS author,
  COALESCE(a.artist_name, 'N/A') AS visual_artist,
  COALESCE(m.artist, 'N/A') AS music_artist,
  COUNT(*) AS works_released,
  y.year
FROM (
  SELECT first_publish_year AS year FROM `inst767-murano-cultural-lens.cultural_lens.books`
  UNION DISTINCT
  SELECT SAFE_CAST(object_date AS INT64) FROM `inst767-murano-cultural-lens.cultural_lens.artwork`
  UNION DISTINCT
  SELECT EXTRACT(YEAR FROM release_date) FROM `inst767-murano-cultural-lens.cultural_lens.music`
) y
LEFT JOIN `inst767-murano-cultural-lens.cultural_lens.books` b ON b.first_publish_year = y.year
LEFT JOIN `inst767-murano-cultural-lens.cultural_lens.artwork` a ON SAFE_CAST(a.object_date AS INT64) = y.year
LEFT JOIN `inst767-murano-cultural-lens.cultural_lens.music` m ON EXTRACT(YEAR FROM m.release_date) = y.year
WHERE y.year = @year
GROUP BY author, visual_artist, music_artist, y.year
ORDER BY works_released DESC
LIMIT 10;

------------------------------ 2. Most Popular Art Mediums by Year (from 1950 onward) ------------------------------
-- For every year from 1950 on, list the most common art medium.
SELECT
  CAST(object_date AS INT64) AS year,
  medium,
  COUNT(*) AS count
FROM `inst767-murano-cultural-lens.cultural_lens.artwork`
WHERE SAFE_CAST(object_date AS INT64) >= 1950
GROUP BY year, medium
QUALIFY ROW_NUMBER() OVER (PARTITION BY year ORDER BY count DESC) = 1
ORDER BY year;

------------------------------ 3. Names Appearing Across Multiple Mediums (e.g., Author and Musician) ------------------------------
-- Find names that appear as both author and musician, or artist and musician, etc.
-- Authors who also appear as music artists
SELECT DISTINCT b.author_name AS name, "Author & Musician" AS cross_medium
FROM `inst767-murano-cultural-lens.cultural_lens.books` b
JOIN `inst767-murano-cultural-lens.cultural_lens.music` m
  ON LOWER(b.author_name) = LOWER(m.artist)

UNION ALL

-- Visual artists who also appear as music artists
SELECT DISTINCT a.artist_name AS name, "Visual Artist & Musician" AS cross_medium
FROM `inst767-murano-cultural-lens.cultural_lens.artwork` a
JOIN `inst767-murano-cultural-lens.cultural_lens.music` m
  ON LOWER(a.artist_name) = LOWER(m.artist)

UNION ALL

-- Authors who are also visual artists
SELECT DISTINCT b.author_name AS name, "Author & Visual Artist" AS cross_medium
FROM `inst767-murano-cultural-lens.cultural_lens.books` b
JOIN `inst767-murano-cultural-lens.cultural_lens.artwork` a
  ON LOWER(b.author_name) = LOWER(a.artist_name)
ORDER BY name, cross_medium;

------------------------------ 4. Year with the Highest Volume of Cultural Output (All Mediums) ------------------------------
-- Identify the year with the highest total number of books, artworks, and music tracks released.
SELECT year, total_works
FROM (
  SELECT year, SUM(count) AS total_works
  FROM (
    SELECT first_publish_year AS year, COUNT(*) AS count
    FROM `inst767-murano-cultural-lens.cultural_lens.books`
    GROUP BY year
    UNION ALL
    SELECT SAFE_CAST(object_date AS INT64) AS year, COUNT(*) AS count
    FROM `inst767-murano-cultural-lens.cultural_lens.artwork`
    GROUP BY year
    UNION ALL
    SELECT EXTRACT(YEAR FROM release_date) AS year, COUNT(*) AS count
    FROM `inst767-murano-cultural-lens.cultural_lens.music`
    GROUP BY year
  )
  GROUP BY year
)
ORDER BY total_works DESC
LIMIT 1;

------------------------------ 5. Books that Coincided with the Highest Number of Cross-Medium Releases ------------------------------
-- Find books whose publication year had the most artworks and music tracks released, showing cultural “hotspots.”
SELECT
  b.title AS book_title,
  b.author_name,
  b.first_publish_year,
  (
    SELECT COUNT(*) 
    FROM `inst767-murano-cultural-lens.cultural_lens.artwork` a
    WHERE SAFE_CAST(a.object_date AS INT64) = b.first_publish_year
  ) AS artwork_count,
  (
    SELECT COUNT(*)
    FROM `inst767-murano-cultural-lens.cultural_lens.music` m
    WHERE EXTRACT(YEAR FROM m.release_date) = b.first_publish_year
  ) AS music_count,
  (
    (
      SELECT COUNT(*) 
      FROM `inst767-murano-cultural-lens.cultural_lens.artwork` a
      WHERE SAFE_CAST(a.object_date AS INT64) = b.first_publish_year
    ) +
    (
      SELECT COUNT(*)
      FROM `inst767-murano-cultural-lens.cultural_lens.music` m
      WHERE EXTRACT(YEAR FROM m.release_date) = b.first_publish_year
    )
  ) AS total_cross_medium_releases
FROM `inst767-murano-cultural-lens.cultural_lens.books` b
ORDER BY total_cross_medium_releases DESC
LIMIT 10;
