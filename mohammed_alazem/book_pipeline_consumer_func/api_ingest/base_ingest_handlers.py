import os
import json
import logging
from google.cloud import pubsub_v1
from typing import List, Dict, Any, Optional, Tuple

# Configure a logger for this module
logger = logging.getLogger(__name__)
# Basic configuration, can be enhanced if a centralized logging setup is adopted
if not logging.getLogger().hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class PubSubPublisherClient:
    """A client to publish messages to a Google Cloud Pub/Sub topic."""
    _publisher_client = None  # Class variable to hold the singleton Pub/Sub client instance

    def __init__(self, topic_path: str):
        """
        Initializes the PubSubPublisherClient.

        Args:
            topic_path: The fully qualified path to the Pub/Sub topic 
                        (e.g., 'projects/your-project-id/topics/your-topic-name').
        """
        self.topic_path = topic_path
        if not self.topic_path:
            raise ValueError("Topic path must be provided for PubSubPublisherClient.")

        # Initialize the actual Google Cloud Pub/Sub client as a singleton
        if PubSubPublisherClient._publisher_client is None:
            logger.info("Initializing Google Cloud Pub/Sub publisher client singleton.")
            PubSubPublisherClient._publisher_client = pubsub_v1.PublisherClient()
        self.client = PubSubPublisherClient._publisher_client

    def publish_message(self, data: List[Dict[str, Any]]) -> Optional[str]:
        """
        Publishes a list of dictionaries (representing messages) to the configured Pub/Sub topic.

        Args:
            data: A list of dictionaries to be published. Each dictionary is a message.
                  The list itself will be JSON serialized for the message data.

        Returns:
            The message ID if successful, None otherwise.
        
        Raises:
            Exception: Re-raises exceptions from the Pub/Sub client if publishing fails.
        """
        if not isinstance(data, list):
            logger.error(f"PubSub Client: Data to publish must be a list of dictionaries. Got {type(data)}.")
            # Depending on strictness, could raise ValueError here
            return None 

        logger.info(f"PubSub Client: Attempting to publish {len(data)} item(s)/entries to {self.topic_path}.")
        try:
            # The entire list is serialized as a single message payload.
            # If individual messages are needed per item in the list, this loop would be outside.
            message_json = json.dumps(data) 
            message_bytes = message_json.encode("utf-8")
            
            future = self.client.publish(self.topic_path, data=message_bytes)
            message_id = future.result(timeout=10)  # Blocking call with timeout
            logger.info(f"PubSub Client: Successfully published message {message_id} to {self.topic_path} containing {len(data)} item(s)/entries.")
            return message_id
        except Exception as e:
            logger.error(f"PubSub Client: Failed to publish message to {self.topic_path}: {e}", exc_info=True)
            raise # Re-raise the exception to be handled by the caller 

def publish_to_pubsub(data: List[Dict[str, Any]], topic_path: str, logger: logging.Logger) -> Tuple[bool, Optional[str]]:
    """
    Publishes data to a Pub/Sub topic.
    
    Args:
        data: List of dictionaries to publish
        topic_path: Full path of the Pub/Sub topic
        logger: Logger instance to use
        
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        publisher = PubSubPublisherClient(topic_path=topic_path)
        publisher.publish_message(data=data)
        logger.info(f"Successfully published {len(data)} items to {topic_path}")
        return True, None
    except Exception as e:
        error_msg = f"Failed to publish to Pub/Sub: {e}"
        logger.error(f"{error_msg}", exc_info=True)
        return False, error_msg 