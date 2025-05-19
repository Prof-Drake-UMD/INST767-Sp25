import json
import csv

with open("merged_output.json") as f:
    raw_data = json.load(f)

# If raw_data contains JSON strings instead of dicts, decode them:
try:
    data = [json.loads(entry) if isinstance(entry, str) else entry for entry in raw_data]
except json.JSONDecodeError:
    print("Some entries could not be decoded. Check your JSON file.")
    exit()

with open("output.csv", "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = [
        "song_title",
        "artist",
        "lyrics_url",
        "lastfm_listeners",
        "lastfm_playcount",
        "temperature_F",
        "weather_main",
        "weather_description",
        "city"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for entry in data:
        writer.writerow({
            "song_title": entry.get("song_title"),
            "artist": entry.get("artist"),
            "lyrics_url": entry.get("lyrics_url"),
            "lastfm_listeners": entry.get("lastfm_listeners"),
            "lastfm_playcount": entry.get("lastfm_playcount"),
            "temperature_F": entry.get("temperature_F"),
            "weather_main": entry.get("weather_main"),
            "weather_description": entry.get("weather_description"),
            "city": entry.get("city")
        })

print("CSV file created: output.csv")

