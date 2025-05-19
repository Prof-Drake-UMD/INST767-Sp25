CREATE TABLE song_weather (
    song_title STRING,
    artist STRING,
    city STRING,
    weather_main STRING,
    temperature FLOAT64,
    weather_description STRING,
    lyrics_preview STRING,         -- instead of lyrics_url
    song_mood STRING,
    weather_mood STRING,
    ingested_at TIMESTAMP
);
