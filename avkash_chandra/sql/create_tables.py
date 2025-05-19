schema_sql = """
CREATE TABLE song_insights (
    song_title STRING,
    artist STRING,
    lyrics_url STRING,
    lastfm_listeners INT64,
    lastfm_playcount STRING,
    temperature_F FLOAT64,
    weather_main STRING,
    weather_description STRING,
    city STRING
);
"""

with open("../Library/Application Support/JetBrains/PyCharm2025.1/scratches/create_tables.sql", "w") as f:
    f.write(schema_sql)

print("SQL table schema saved to create_tables.sql")
