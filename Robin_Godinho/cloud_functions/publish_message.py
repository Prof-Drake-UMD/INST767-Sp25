from google.cloud import pubsub_v1
import json

project_id = "inst737-final-project"
topic_id = "real-news-ingest"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

message_data = {
    "filename": "data/cleaned/newsdata.csv",
    "table": "real_news_data.newsdata"  # <-- Make sure this matches your dataset/table name
}

message_json = json.dumps(message_data).encode("utf-8")
future = publisher.publish(topic_path, data=message_json)
print(f"✅ Published message to {topic_path}")
print(f"📦 Payload: {message_data}")
