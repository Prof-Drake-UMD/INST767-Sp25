
-- 1. Most listened songs
SELECT song_title, artist, lastfm_listeners
FROM song_insights
ORDER BY lastfm_listeners DESC
LIMIT 5;

-- 2. Songs listened to in cold weather
SELECT song_title, artist, temperature_F
FROM song_insights
WHERE temperature_F < 50;

-- 3. Songs with overcast clouds
SELECT song_title, artist, weather_description
FROM song_insights
WHERE weather_description LIKE '%overcast%';

-- 4. Unique weather conditions for most-played songs
SELECT DISTINCT weather_description
FROM song_insights
WHERE lastfm_playcount != 'N/A';

-- 5. Count songs by weather type
SELECT weather_main, COUNT(*) as song_count
FROM song_insights
GROUP BY weather_main
ORDER BY song_count DESC;
