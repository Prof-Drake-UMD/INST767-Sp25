"""
# ISSUE #5: STANDALONE SUBSCRIBER SERVICE
Standalone subscriber service for listening to transit data events.
This can be deployed as a separate service (e.g., Cloud Run) to process messages asynchronously.
"""
import os
import logging
import time
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

# Import project modules
from pubsub_messaging import subscribe_callback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get environment variables
project_id = os.environ.get('GCP_PROJECT', 'lithe-camp-458100-s5')
subscription_id = os.environ.get('PUBSUB_SUBSCRIPTION', 'transit-data-subscription')

def main():
    """Main function to start the subscriber - ISSUE #5: Standalone subscriber service"""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    # How many messages to pull at once
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    
    logger.info(f"Starting subscriber for {subscription_path}")
    
    # ISSUE #5: Subscribe to the Pub/Sub topic and define callback
    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=subscribe_callback,
        flow_control=flow_control
    )
    
    # Block to keep the subscriber alive
    try:
        logger.info("Subscriber started. Waiting for messages...")
        # ISSUE #5: Keep the subscriber running to process messages continuously
        streaming_pull_future.result()
    except KeyboardInterrupt:
        logger.info("Subscriber stopped by user")
        streaming_pull_future.cancel()
    except TimeoutError:
        logger.error("Subscriber timed out")
        streaming_pull_future.cancel()
    except Exception as e:
        logger.error(f"Subscriber error: {e}")
        streaming_pull_future.cancel()
    
    subscriber.close()
    logger.info("Subscriber service shut down")

if __name__ == "__main__":
    main()