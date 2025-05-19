import json
from Lastfm import get_lastfm_data
from Genius import get_genius_data
from Openweather import get_weather_data

# 🧠 Sample list of songs and corresponding cities
entries = [
    {"search_text": "Blinding Lights", "city": "Los Angeles"},
    {"search_text": "As It Was", "city": "New York"},
    {"search_text": "Love Me Again", "city": "College Park"},
    {"search_text": "Paint The Town Red", "city": "Miami"},
    {"search_text": "Cruel Summer", "city": "Chicago"}
]

dataset = []

for entry in entries:
    print(f"\nFetching data for '{entry['search_text']}' in {entry['city']}...")

    lastfm_data = get_lastfm_data(entry['search_text'])
    genius_data = get_genius_data(entry['search_text'])
    weather_data = get_weather_data(entry['city'])

    # 🧩 Merge all data
    merged = {
        "song_title": lastfm_data.get("song_title"),
        "artist": lastfm_data.get("artist"),
        "lyrics_url": genius_data.get("lyrics_url"),
        "lastfm_listeners": lastfm_data.get("listeners"),
        "lastfm_playcount": "N/A",  # Optional: no playcount in this endpoint
        "temperature_F": weather_data.get("temperature_F"),
        "weather_main": weather_data.get("weather"),
        "weather_description": weather_data.get("description"),
        "city": weather_data.get("city")
    }

    dataset.append(merged)

# 💾 Save the dataset
with open("merged_output.json", "w") as f:
    json.dump(dataset, f, indent=4)

print(f"\nCreated dataset with {len(dataset)} records: saved to merged_output.json")



