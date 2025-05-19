-- 1. Top 5 songs played most frequently (by count of rows) with no duplicates by song+artist
SELECT song_title, artist, COUNT(*) AS play_count
FROM (
  SELECT DISTINCT song_title, artist, city, temperature, weather_main, ingested_at
  FROM `music-insights-project.music_dataset.song_weather`
)
GROUP BY song_title, artist
ORDER BY play_count DESC
LIMIT 5;

-- 2. Songs played when temperature was below 50°F, deduplicated
SELECT DISTINCT song_title, artist, temperature, weather_main, city
FROM `music-insights-project.music_dataset.song_weather`
WHERE temperature < 50
ORDER BY temperature ASC;

-- 3. Songs played in "Clouds" weather, no duplicates
SELECT DISTINCT song_title, artist, temperature, city
FROM `music-insights-project.music_dataset.song_weather`
WHERE weather_main = 'Clouds'
ORDER BY temperature DESC;

-- 4. Unique weather conditions for songs with play count >= 2 (as proxy for most-played)
SELECT DISTINCT weather_main
FROM (
  SELECT song_title, artist, weather_main, COUNT(*) AS play_count
  FROM `music-insights-project.music_dataset.song_weather`
  GROUP BY song_title, artist, weather_main
  HAVING play_count >= 2
);

-- 5. Count of unique songs by weather condition
SELECT weather_main, COUNT(DISTINCT CONCAT(song_title, ' - ', artist)) AS unique_songs_count
FROM `music-insights-project.music_dataset.song_weather`
GROUP BY weather_main
ORDER BY unique_songs_count DESC;


with open("../Library/Application Support/JetBrains/PyCharm2025.1/scratches/queries.sql", "w") as f:
    f.write(queries)

print("Example queries saved to queries.sql")
