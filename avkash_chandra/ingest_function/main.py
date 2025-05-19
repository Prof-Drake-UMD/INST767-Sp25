import json
import requests
from Lastfm import get_lastfm_data
from Genius import get_genius_data
from Openweather import get_weather_data

def get_top_songs(limit=25):
    api_key = "89afa8b67db8d932ae8b8ef09300d480"
    url = f"http://ws.audioscrobbler.com/2.0/?method=chart.gettoptracks&api_key={api_key}&format=json&limit={limit}"
    response = requests.get(url)
    top_songs = []

    if response.status_code == 200:
        tracks = response.json()["tracks"]["track"]
        for track in tracks:
            top_songs.append({
                "search_text": track["name"],
                "artist": track["artist"]["name"]
            })
    else:
        print("Failed to fetch top songs.")
    return top_songs

def build_dataset():
    top_songs = get_top_songs(limit=25)
    cities = ["New York", "Los Angeles", "Miami", "Chicago", "Seattle", "Austin", "Atlanta", "San Francisco", "Denver", "Boston"]

    dataset = []

    for i, entry in enumerate(top_songs):
        city = cities[i % len(cities)]
        print(f"\nFetching data for '{entry['search_text']}' by {entry['artist']} in {city}...")

        lastfm_data = get_lastfm_data(entry['search_text'])
        genius_data = get_genius_data(entry['search_text'], artist_name=entry['artist'])
        weather_data = get_weather_data(city)

        # Extract lyrics_preview safely
        lyrics_preview = genius_data.get("lyrics_preview", "N/A")

        merged = {
            "song_title": lastfm_data.get("song_title"),
            "artist": lastfm_data.get("artist"),
            "lyrics_url": genius_data.get("lyrics_url"),
            "lyrics_preview": lyrics_preview,
            "lastfm_listeners": lastfm_data.get("listeners"),
            "lastfm_playcount": "N/A",  # Or get real value if available
            "temperature_F": weather_data.get("temperature_F"),
            "weather_main": weather_data.get("weather"),
            "weather_description": weather_data.get("description"),
            "city": weather_data.get("city")
        }

        dataset.append(merged)

    with open("/tmp/merged_output.json", "w") as f:
        json.dump(dataset, f, indent=4)

    print(f"\nCreated dataset with {len(dataset)} records: saved to /tmp/merged_output.json")

def ingest_entry_point(request):
    build_dataset()
    return "Ingest complete", 200

