import json

# Load data from each file
with open("lastfm_output.json") as f:
    lastfm_data = json.load(f)

with open("genius_song_data.json") as f:
    genius_data = json.load(f)

with open("weather_data.json") as f:
    weather_data = json.load(f)

# Merge into one dictionary
merged_data = {
    "song_title": lastfm_data["song_title"],
    "artist": lastfm_data["artist"],
    "lyrics_url": genius_data.get("lyrics_url", "N/A"),
    "lastfm_listeners": lastfm_data.get("listeners", "N/A"),
    "lastfm_playcount": "N/A",  # still not available
    "weather": {
        "city": weather_data.get("city", "Unknown"),
        "temperature_F": weather_data.get("temperature_F", "N/A"),
        "weather": weather_data.get("weather", "N/A"),
        "description": weather_data.get("description", "N/A")
    }
}

# Save merged output
with open("merged_output.json", "w") as f:
    json.dump(merged_data, f, indent=4)

print("Merged data saved to merged_output.json")
