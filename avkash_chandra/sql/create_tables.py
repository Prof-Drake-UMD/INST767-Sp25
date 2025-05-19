schema_sql = """
CREATE TABLE song_weather (
    song_title STRING,
    artist STRING,
    city STRING,
    weather_main STRING,
    temperature FLOAT64,
    weather_description STRING,
    lyrics_preview STRING,
    song_mood STRING,
    weather_mood STRING,
    lastfm_listeners INT64,
    lastfm_playcount STRING,
    ingested_at TIMESTAMP
);

"""

with open("../Library/Application Support/JetBrains/PyCharm2025.1/scratches/create_tables.sql", "w") as f:
    f.write(schema_sql)

print("SQL table schema saved to create_tables.sql")
