# What types of visual art were most common during the decade a book was published?

SELECT
  FLOOR(book_first_publish_year / 10) * 10 AS decade,
  artwork_medium,
  COUNT(*) AS count
FROM cultural_experience_items
WHERE artwork_medium IS NOT NULL
GROUP BY decade, artwork_medium
ORDER BY decade, count DESC;

# Top 5 Unintentional Cross-Media Collaborators

SELECT
  artwork_artist,
  track_artist,
  COUNT(*) AS co_occurrences,
  ARRAY_AGG(DISTINCT book_first_publish_year ORDER BY book_first_publish_year) AS shared_years
FROM cultural_experience_items
WHERE artwork_artist IS NOT NULL AND track_artist IS NOT NULL
  AND SAFE_CAST(SUBSTR(artwork_date, 1, 4) AS INT64) = SAFE_CAST(SUBSTR(track_release_date, 1, 4) AS INT64)
GROUP BY artwork_artist, track_artist
HAVING co_occurrences > 1
ORDER BY co_occurrences DESC
LIMIT 5;

#  Cultural Cross-Media Snapshots from Culturally Significant Years

SELECT
  book_title,
  book_author,
  book_first_publish_year,
  
  artwork_title,
  artwork_artist,
  artwork_medium,
  artwork_date,
  artwork_url,

  track_title,
  track_artist,
  album_title,
  track_release_date

FROM cultural_experience_items

WHERE book_first_publish_year IN (1968, 1989, 2001)
  AND SAFE_CAST(SUBSTR(track_release_date, 1, 4) AS INT64) = book_first_publish_year
  AND SAFE_CAST(SUBSTR(artwork_date, 1, 4) AS INT64) = book_first_publish_year

ORDER BY book_first_publish_year, book_title, track_artist;

#  Cultural Snapshot for the Publication Year of a Given Book

WITH book_year AS (
  SELECT book_first_publish_year AS year
  FROM cultural_experience_items
  WHERE LOWER(book_title) = LOWER('Normal People')
  LIMIT 1
),

medium_counts AS (
  SELECT
    artwork_medium,
    COUNT(*) AS count
  FROM cultural_experience_items
  WHERE SAFE_CAST(SUBSTR(artwork_date, 1, 4) AS INT64) = (SELECT year FROM book_year)
    AND artwork_medium IS NOT NULL
  GROUP BY artwork_medium
  ORDER BY count DESC
  LIMIT 1
),

genre_counts AS (
  SELECT
    album_title AS genre_estimate,  
    COUNT(*) AS count
  FROM cultural_experience_items
  WHERE SAFE_CAST(SUBSTR(track_release_date, 1, 4) AS INT64) = (SELECT year FROM book_year)
    AND album_title IS NOT NULL
  GROUP BY album_title
  ORDER BY count DESC
  LIMIT 1
)

SELECT
  (SELECT year FROM book_year) AS book_year,
  (SELECT artwork_medium FROM medium_counts) AS most_common_art_medium,
  (SELECT genre_estimate FROM genre_counts) AS most_common_music_album


# Cultural Snapshot for the publication year of a submitted book

WITH book_year AS (
  SELECT book_first_publish_year AS year
  FROM cultural_experience_items
  WHERE LOWER(book_title) = LOWER('Normal People')
  LIMIT 1
),

top_medium AS (
  SELECT
    artwork_medium,
    COUNT(*) AS count
  FROM cultural_experience_items
  WHERE SAFE_CAST(SUBSTR(artwork_date, 1, 4)) = (SELECT year FROM book_year)
    AND artwork_medium IS NOT NULL
  GROUP BY artwork_medium
  ORDER BY count DESC
  LIMIT 1
),

top_artists AS (
  SELECT
    track_artist,
    COUNT(*) AS count
  FROM cultural_experience_items
  WHERE SAFE_CAST(SUBSTR(track_release_date, 1, 4)) = (SELECT year FROM book_year)
    AND track_artist IS NOT NULL
  GROUP BY track_artist
  ORDER BY count DESC
  LIMIT 3
)

SELECT
  (SELECT year FROM book_year) AS cultural_year,
  (SELECT artwork_medium FROM top_medium) AS most_common_art_medium,
  ARRAY_AGG(track_artist) AS most_common_music_artists
FROM top_artists;

# Authors that also released music or created visual art

SELECT
  book_author,
  COUNT(DISTINCT CASE WHEN book_author = track_artist THEN track_title END) AS music_works,
  COUNT(DISTINCT CASE WHEN book_author = artwork_artist THEN artwork_title END) AS visual_works
FROM cultural_experience_items
WHERE book_author IS NOT NULL
GROUP BY book_author
HAVING music_works > 0 OR visual_works > 0
ORDER BY (music_works + visual_works) DESC;
