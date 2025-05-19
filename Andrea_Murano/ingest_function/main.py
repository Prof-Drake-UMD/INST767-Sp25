import os
import sys
import json
from google.cloud import pubsub_v1
from flask import Flask, request, jsonify

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api_logic import match_cultural_experience_by_year

app = Flask(__name__)

PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "ingest-to-transform")
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")

@app.route("/", methods=["POST"])
def ingest():
    data = request.get_json()
    if not data or not data.get("book_title") or not data.get("author_name"):
        return jsonify({"error": "Missing book_title or author_name"}), 400
    try:
        # Call your API aggregation function
        result = match_cultural_experience_by_year(data["book_title"], data["author_name"])
    except Exception as e:
        return jsonify({"error": "Failed to fetch API data", "details": str(e)}), 500

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)
        message_json = json.dumps(result)
        publisher.publish(topic_path, data=message_json.encode("utf-8"))
        return jsonify({"status": "Message published", "data": result}), 200
    except Exception as e:
        return jsonify({"error": "Failed to publish to Pub/Sub", "details": str(e)}), 500
