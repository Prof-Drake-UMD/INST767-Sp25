from google.cloud import pubsub_v1
import json

project_id = "music-insights-project"
topic_id = "song-ingest-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

message_dict = {
    "song_title": "Test Song",
    "artist": "Test Artist",
    "city": "Seattle",
    "weather_main": "Rain",
    "temperature": 58.0,
    "lyrics_preview": "I cry every night, alone in the dark"
}

message_json = json.dumps(message_dict).encode("utf-8")

future = publisher.publish(topic_path, data=message_json)
print(f"Published message ID: {future.result()}")
