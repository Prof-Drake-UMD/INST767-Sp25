# Query 1: Most common key words across titles, grouped by cultural era.

WITH all_titles AS (
  SELECT theme AS era, LOWER(title) AS raw_title
  FROM cultural_items
  WHERE theme_type = 'eras'
  
  UNION ALL
  
  SELECT theme AS era, LOWER(album_title)
  FROM cultural_items
  WHERE theme_type = 'eras' AND album_title IS NOT NULL
),
tokenized AS (
  SELECT
    era,
    word
  FROM all_titles,
  UNNEST(SPLIT(REGEXP_REPLACE(raw_title, r'[^a-zA-Z0-9\s]', ''), ' ')) AS word
),
filtered AS (
  SELECT
    era,
    word
  FROM tokenized
  WHERE word NOT IN (
    'a', 'an', 'the', 'is', 'of', 'and', 'in', 'to', 'on', 'for',
    'with', 'by', 'at', 'as', 'from', 'this', 'that', 'it', ''
  )
  AND LENGTH(word) > 2
)
SELECT
  era,
  word,
  COUNT(*) AS count
FROM filtered
GROUP BY era, word
ORDER BY era, count DESC;

# Query 2: Which cultural eras are most strongly associated with overarching themes?
SELECT
  e.theme AS era,
  t.theme AS timeless_theme,
  COUNT(*) AS count
FROM cultural_items e
JOIN cultural_items t
  ON e.item_id = t.item_id
WHERE e.theme_type = 'eras' AND t.theme_type = 'themes'
GROUP BY era, timeless_theme
ORDER BY count DESC;

# Query 3: Which countries are most strongly associated with overarching themes?
SELECT
  g.theme AS geography,
  t.theme AS timeless_theme,
  COUNT(*) AS count
FROM cultural_items g
JOIN cultural_items t ON g.item_id = t.item_id
WHERE g.theme_type = 'geography' AND t.theme_type = 'themes'
GROUP BY g.theme, t.theme
ORDER BY count DESC;

# Query 4: Who created the most work in each reason, in each era?
SELECT
  era.theme AS era,
  geo.theme AS geography,
  creator_name,
  COUNT(*) AS work_count
FROM
  cultural_items AS era
JOIN
  cultural_items AS geo
  ON era.item_id = geo.item_id
WHERE
  era.theme_type = 'eras'
  AND geo.theme_type = 'geography'
  AND creator_name IS NOT NULL
GROUP BY
  era.theme, geo.theme, creator_name
ORDER BY
  era.theme, geo.theme, work_count DESC;

