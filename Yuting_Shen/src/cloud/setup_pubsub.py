"""
Script to set up Google Cloud Pub/Sub topics and subscriptions.
"""
import os
import argparse
import logging
from google.cloud import pubsub_v1


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_pubsub(project_id):
    """
    Set up Pub/Sub topics and subscriptions for the pipeline.

    Args:
        project_id (str): Google Cloud Project ID
    """
    # Create publisher client
    publisher = pubsub_v1.PublisherClient()

    # Create topics
    topics = [
        'sports-data-ingest',  # Published after sports data is ingested
        'youtube-data-ingest',  # Published after YouTube data is ingested
        'trends-data-ingest',  # Published after trends data is ingested
        'transform-complete'  # Published after data is transformed
    ]

    for topic_name in topics:
        topic_path = publisher.topic_path(project_id, topic_name)
        try:
            topic = publisher.create_topic(name=topic_path)
            logger.info(f"Created topic: {topic.name}")
        except Exception as e:
            logger.info(f"Topic already exists: {topic_path}")

    # Create subscriber client
    subscriber = pubsub_v1.SubscriberClient()

    # Create subscriptions
    subscriptions = [
        ('transform-sports-subscription', 'sports-data-ingest'),
        ('transform-youtube-subscription', 'youtube-data-ingest'),
        ('transform-trends-subscription', 'trends-data-ingest'),
        ('load-subscription', 'transform-complete')
    ]

    for sub_name, topic_name in subscriptions:
        subscription_path = subscriber.subscription_path(project_id, sub_name)
        topic_path = publisher.topic_path(project_id, topic_name)

        try:
            subscription = subscriber.create_subscription(
                name=subscription_path, topic=topic_path
            )
            logger.info(f"Created subscription: {subscription.name}")
        except Exception as e:
            logger.info(f"Subscription already exists: {subscription_path}")


def main():
    parser = argparse.ArgumentParser(description="Set up Pub/Sub topics and subscriptions")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")

    args = parser.parse_args()
    setup_pubsub(args.project_id)


if __name__ == "__main__":
    main()