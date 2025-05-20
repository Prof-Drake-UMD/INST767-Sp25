from flask import Request, jsonify
import os
import json
from google.cloud import pubsub_v1
from api_logic import match_cultural_experience_by_year
from dotenv import load_dotenv
import os

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
GCP_PROJECT = os.getenv("GCP_PROJECT")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "ingest-to-transform")
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")

def ingest(request: Request):
    if request.method != "POST":
        return jsonify({"error": "Only POST allowed"}), 405
    data = request.get_json()
    if not data or not data.get("book_title") or not data.get("author_name"):
        return jsonify({"error": "Missing book_title or author_name"}), 400

    try:
        result = match_cultural_experience_by_year(data["book_title"], data["author_name"])
    except Exception as e:
        return jsonify({"error": "Failed to fetch API data", "details": str(e)}), 500

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
        publisher.publish(topic_path, data=json.dumps(result).encode("utf-8"))
        return jsonify({"status": "Message published", "data": result}), 200
    except Exception as e:
        return jsonify({"error": "Failed to publish to Pub/Sub", "details": str(e)}), 500
