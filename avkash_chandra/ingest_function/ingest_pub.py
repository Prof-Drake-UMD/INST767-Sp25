from google.cloud import pubsub_v1
import json
from Genius import get_genius_data
import main  # your dataset builder

PROJECT_ID = "music-insights-project"
TOPIC_ID = "song-ingest-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def is_valid_record(song_data):
    try:
        temp = song_data.get("temperature_F") or song_data.get("temperature")
        if temp is None:
            return False
        float(temp)  # check it's a number
    except (ValueError, TypeError):
        return False

    required_keys = ["song_title", "artist", "city", "weather_main"]
    return all(song_data.get(k) for k in required_keys)

def publish_song_data(song_data, seen_keys):
    key = (song_data.get("song_title"), song_data.get("artist"), song_data.get("city"))
    if key in seen_keys:
        print(f"Skipping duplicate: {key}")
        return

    if not is_valid_record(song_data):
        p3rint(f"Skipping invalid weather or data: {song_data}")
        return

    genius_info = get_genius_data(song_data["song_title"])
    song_data["lyrics_preview"] = genius_info.get("lyrics_preview", "No preview available")

    message_json = json.dumps(song_data)
    message_bytes = message_json.encode("utf-8")
    future = publisher.publish(topic_path, data=message_bytes)
    print(f"Published: {key} → {future.result()}")
    seen_keys.add(key)

def main_ingest():
    main.build_dataset()
    with open("/tmp/merged_output.json", "r") as f:
        dataset = json.load(f)

    seen_keys = set()
    for song_data in dataset:
        publish_song_data(song_data, seen_keys)

if __name__ == "__main__":
    main_ingest()


