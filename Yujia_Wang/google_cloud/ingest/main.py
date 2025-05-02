from fetch import gather_movie_full_data
from google.cloud import pubsub_v1
import os
import json

def main_entry(request):
    topic_name = os.environ.get("TOPIC_NAME")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.environ["PROJECT_ID"], topic_name)

    movies = gather_movie_full_data(region='US')
    for movie in movies:
        movie_data = json.dumps(movie).encode("utf-8")
        publisher.publish(topic_path, movie_data)

    return f"✅ Published {len(movies)} movies to Pub/Sub topic: {topic_name}"
