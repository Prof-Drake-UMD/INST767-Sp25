"""
# ISSUE #5: PUB/SUB MESSAGING MODULE
Module for publishing and subscribing to Pub/Sub messages.
This enables asynchronous communication between the ingest and transform steps.
"""
import json
import os
import logging
import datetime
from google.cloud import pubsub_v1
from google.cloud import storage
from google.api_core.exceptions import AlreadyExists

# Configure logging
logger = logging.getLogger(__name__)

# Get environment variables
project_id = os.environ.get('GCP_PROJECT', 'lithe-camp-458100-s5')
topic_id = os.environ.get('PUBSUB_TOPIC', 'transit-data-events')
subscription_id = os.environ.get('PUBSUB_SUBSCRIPTION', 'transit-data-subscription')

# ISSUE #5: Function to initialize Pub/Sub resources
def initialize_pubsub():
    """Initialize Pub/Sub topic and subscription if they don't exist - ISSUE #5: Pub/Sub setup"""
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    
    topic_path = publisher.topic_path(project_id, topic_id)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    # Create the topic if it doesn't exist
    try:
        publisher.create_topic(request={"name": topic_path})
        logger.info(f"Created topic: {topic_path}")
    except AlreadyExists:
        logger.info(f"Topic already exists: {topic_path}")
    
    # Create the subscription if it doesn't exist
    try:
        subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path}
        )
        logger.info(f"Created subscription: {subscription_path}")
    except AlreadyExists:
        logger.info(f"Subscription already exists: {subscription_path}")
    
    return publisher, subscriber, topic_path, subscription_path

# ISSUE #5: Function to publish a message to Pub/Sub
def publish_message(publisher, topic_path, message_dict):
    """Publish a message to the Pub/Sub topic - ISSUE #5: Message publishing"""
    try:
        # Convert the message to JSON
        message_json = json.dumps(message_dict).encode('utf-8')
        
        # Publish the message
        future = publisher.publish(topic_path, data=message_json)
        message_id = future.result()
        
        logger.info(f"Published message: {message_id}")
        return message_id
    except Exception as e:
        logger.error(f"Error publishing message: {e}")
        raise

# ISSUE #5: Function to publish a transit data event
def publish_transit_data_event(source, data_type, gcs_uri):
    """
    Publish a transit data event to the Pub/Sub topic - ISSUE #5: Event publishing
    
    Args:
        source (str): The data source (mbta, wmata, cta)
        data_type (str): The type of data (routes, predictions, etc.)
        gcs_uri (str): The GCS URI for the raw data
    """
    # Initialize Pub/Sub
    publisher, _, topic_path, _ = initialize_pubsub()
    
    # Create message payload
    message = {
        'source': source,
        'data_type': data_type,
        'gcs_uri': gcs_uri,
        'timestamp': datetime.datetime.now().isoformat()
    }
    
    # Publish the message
    return publish_message(publisher, topic_path, message)

# ISSUE #5: Callback function for message processing
def subscribe_callback(message):
    """
    Callback function for Pub/Sub subscription messages - ISSUE #5: Message subscription
    This would be used in a subscriber service to process messages asynchronously.
    
    Args:
        message: The Pub/Sub message object
    """
    try:
        # Decode and parse the message
        data = json.loads(message.data.decode('utf-8'))
        
        logger.info(f"Received message: {data}")
        
        # Process the message based on its content
        source = data.get('source')
        data_type = data.get('data_type')
        gcs_uri = data.get('gcs_uri')
        
        # Handle different data sources
        if source == 'mbta':
            if data_type == 'routes':
                process_mbta_routes(gcs_uri)
            elif data_type == 'predictions':
                process_mbta_predictions(gcs_uri)
        elif source == 'wmata':
            if data_type == 'rail_predictions':
                process_wmata_rail_predictions(gcs_uri)
        elif source == 'cta':
            if data_type == 'train_positions':
                process_cta_train_positions(gcs_uri)
        
        # Acknowledge the message
        message.ack()
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledgment to retry later
        message.nack()

# ISSUE #5: Functions to process different data types after receiving a message
def process_mbta_routes(gcs_uri):
    """Process MBTA routes data from GCS - ISSUE #5: Asynchronous processing"""
    from data_transformers import transform_mbta_routes, save_to_gcs, load_to_bigquery
    from config import BQ_CONFIG
    
    # Read data from GCS
    raw_data = read_from_gcs(gcs_uri)
    
    # Transform data
    transformed_data = transform_mbta_routes(raw_data)
    
    # Save to GCS and load to BigQuery
    new_gcs_path = save_to_gcs(transformed_data, 'mbta', 'routes')
    load_to_bigquery(new_gcs_path, BQ_CONFIG['tables']['mbta'])

def process_mbta_predictions(gcs_uri):
    """Process MBTA predictions data from GCS - ISSUE #5: Asynchronous processing"""
    from data_transformers import transform_mbta_predictions, save_to_gcs, load_to_bigquery
    from config import BQ_CONFIG
    
    # Read data from GCS
    raw_data = read_from_gcs(gcs_uri)
    
    # Transform data
    transformed_data = transform_mbta_predictions(raw_data)
    
    # Save to GCS and load to BigQuery
    new_gcs_path = save_to_gcs(transformed_data, 'mbta', 'predictions')
    load_to_bigquery(new_gcs_path, BQ_CONFIG['tables']['mbta'])

def process_wmata_rail_predictions(gcs_uri):
    """Process WMATA rail predictions data from GCS - ISSUE #5: Asynchronous processing"""
    from data_transformers import transform_wmata_rail_predictions, save_to_gcs, load_to_bigquery
    from config import BQ_CONFIG
    
    # Read data from GCS
    raw_data = read_from_gcs(gcs_uri)
    
    # Transform data
    transformed_data = transform_wmata_rail_predictions(raw_data)
    
    # Save to GCS and load to BigQuery
    new_gcs_path = save_to_gcs(transformed_data, 'wmata', 'rail_predictions')
    load_to_bigquery(new_gcs_path, BQ_CONFIG['tables']['wmata'])

def process_cta_train_positions(gcs_uri):
    """Process CTA train positions data from GCS - ISSUE #5: Asynchronous processing"""
    from data_transformers import transform_cta_train_positions, save_to_gcs, load_to_bigquery
    from config import BQ_CONFIG
    
    # Read data from GCS
    raw_data = read_from_gcs(gcs_uri)
    
    # Transform data
    transformed_data = transform_cta_train_positions(raw_data)
    
    # Save to GCS and load to BigQuery
    new_gcs_path = save_to_gcs(transformed_data, 'cta', 'train_positions')
    load_to_bigquery(new_gcs_path, BQ_CONFIG['tables']['cta_positions'])

# ISSUE #5: Helper function to read data from GCS
def read_from_gcs(gcs_uri):
    """Read data from Google Cloud Storage - ISSUE #5: GCS integration"""
    try:
        # Extract bucket and object name from URI
        uri_parts = gcs_uri.replace('gs://', '').split('/')
        bucket_name = uri_parts[0]
        object_name = '/'.join(uri_parts[1:])
        
        # Get blob and download as string
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        content = blob.download_as_text()
        
        # Parse JSON
        return json.loads(content)
        
    except Exception as e:
        logger.error(f"Error reading from GCS: {e}")
        raise

# ISSUE #5: Function to start a subscriber for long-running subscription
def start_subscriber():
    """Start a subscriber to listen for messages - ISSUE #5: Subscription service"""
    _, subscriber, _, subscription_path = initialize_pubsub()
    
    # Create a callback function
    def callback(message):
        subscribe_callback(message)
    
    # Subscribe to the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info(f"Listening for messages on {subscription_path}")
    
    # Block to keep the subscriber alive
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        logger.info("Subscriber stopped")
    except Exception as e:
        streaming_pull_future.cancel()
        logger.error(f"Subscriber error: {e}")

if __name__ == "__main__":
    # This can be run as a standalone subscriber service - ISSUE #5
    logging.basicConfig(level=logging.INFO)
    start_subscriber()