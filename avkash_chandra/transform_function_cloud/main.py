import base64
import json
from google.cloud import bigquery
from datetime import datetime

# Initialize BigQuery client
client = bigquery.Client()
dataset_id = 'music_dataset'
table_id = 'song_weather'


# Helper to parse float safely
def parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# Mood mapping based on weather
def get_weather_mood(weather):
    mood_map = {
        'Clear': 'Happy',
        'Clouds': 'Calm',
        'Rain': 'Melancholy',
        'Snow': 'Peaceful',
        'Thunderstorm': 'Angry',
        'Drizzle': 'Gloomy',
        'Mist': 'Sleepy',
        'Fog': 'Dreary',
        'Haze': 'Dreamy'
    }
    return mood_map.get(weather, 'Neutral')


# Mood inference based on lyrics
def get_song_mood(lyrics):
    if not lyrics:
        return 'Unknown'
    lyrics = lyrics.lower()
    if 'love' in lyrics:
        return 'Romantic'
    elif 'cry' in lyrics or 'tears' in lyrics:
        return 'Sad'
    elif 'dance' in lyrics or 'party' in lyrics:
        return 'Energetic'
    elif 'alone' in lyrics:
        return 'Lonely'
    elif 'happy' in lyrics or 'smile' in lyrics:
        return 'Happy'
    elif 'night' in lyrics or 'dark' in lyrics:
        return 'Mysterious'
    else:
        return 'Neutral'


def row_exists(song_title, artist, city, ingested_at_minute):
    """
    Check if a row with same song_title, artist, city, and ingested_at truncated to minute exists in BigQuery.
    This is a basic deduplication method to avoid inserting duplicates.
    """
    query = f"""
    SELECT COUNT(*) as count
    FROM `{client.project}.{dataset_id}.{table_id}`
    WHERE song_title = @song_title
      AND artist = @artist
      AND city = @city
      AND FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M', ingested_at) = @ingested_at_minute
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("song_title", "STRING", song_title),
            bigquery.ScalarQueryParameter("artist", "STRING", artist),
            bigquery.ScalarQueryParameter("city", "STRING", city),
            bigquery.ScalarQueryParameter("ingested_at_minute", "STRING", ingested_at_minute),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    for row in results:
        if row.count > 0:
            return True
    return False


# Cloud Function entry point
def process_pubsub_message(event, context):
    if 'data' not in event:
        print("No data found in event.")
        return

    # Decode the base64 message
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(f"Received message: {pubsub_message}")

    try:
        message = json.loads(pubsub_message)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return

    # Parse temperature
    temperature_raw = message.get("temperature_F") or message.get("temperature")
    temperature_value = parse_float(temperature_raw)
    if temperature_value is None:
        print("Warning: temperature is missing or invalid; inserting row with NULL temperature.")

    # Build ingested_at timestamp
    ingested_at = datetime.utcnow()
    ingested_at_iso = ingested_at.isoformat() + 'Z'
    # For dedup check, truncate to minute string (e.g. '2025-05-19T03:31')
    ingested_at_minute = ingested_at.strftime('%Y-%m-%dT%H:%M')

    # Deduplication check
    exists = row_exists(
        song_title=message.get("song_title"),
        artist=message.get("artist"),
        city=message.get("city"),
        ingested_at_minute=ingested_at_minute
    )
    if exists:
        print("Duplicate record detected for this minute, skipping insert.")
        return

    # Enrich with mood data
    song_mood = get_song_mood(message.get("lyrics_preview"))
    weather_mood = get_weather_mood(message.get("weather_main"))

    # Debug print for sanity check
    print(f"DEBUG: song_mood={song_mood}, weather_mood={weather_mood}")

    # Build row for BigQuery
    row = {
        "song_title": message.get("song_title"),
        "artist": message.get("artist"),
        "city": message.get("city"),
        "weather_main": message.get("weather_main"),
        "temperature": temperature_value,
        "ingested_at": ingested_at_iso,
        "lyrics_preview": message.get("lyrics_preview"),
        "song_mood": song_mood,
        "weather_mood": weather_mood
    }

    # Insert row into BigQuery
    errors = client.insert_rows_json(f"{client.project}.{dataset_id}.{table_id}", [row])
    if errors:
        print(f"Errors inserting row into BigQuery: {errors}")
    else:
        print("Inserted row into BigQuery:", row)


