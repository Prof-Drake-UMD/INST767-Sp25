# google_cloud/modified_api_helpers.py
"""
# ISSUE #5: MODIFIED API HELPERS WITH PUB/SUB INTEGRATION
Modified version of api_helpers that integrates with Pub/Sub messaging.
This enables asynchronous communication between ingest and transform steps.
"""
import requests
import json
import logging
import os
from datetime import datetime
from google.cloud import storage

# Import original helpers
from api_helpers import (
    fetch_mbta_data, fetch_wmata_data, fetch_cta_data, 
    get_mbta_routes, get_mbta_predictions, get_wmata_rail_predictions, 
    get_cta_train_positions
)

# ISSUE #5: Import messaging module for Pub/Sub integration
from google_cloud.pubsub_messaging import publish_transit_data_event

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get environment variables
project_id = os.environ.get('GCP_PROJECT', 'lithe-camp-458100-s5')
bucket_name = os.environ.get('GCS_BUCKET', 'transit-data-bucket-767projt0-512xsics3-3x9289-03usbc')

# ISSUE #5: Save data to GCS before publishing to Pub/Sub
def save_to_gcs(data, source, data_type):
    """Save data to Google Cloud Storage - ISSUE #5: Store data for async processing"""
    try:
        # Create a GCS client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        # Generate a timestamp for the filename
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        blob_name = f"raw/{source}/{data_type}_{timestamp}.json"
        blob = bucket.blob(blob_name)
        
        # Convert data to JSON string
        json_data = json.dumps(data)
        
        # Upload to GCS
        blob.upload_from_string(json_data, content_type='application/json')
        
        # Log success
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        logger.info(f"Saved {source} {data_type} data to {gcs_uri}")
        
        return gcs_uri
    
    except Exception as e:
        logger.error(f"Error saving data to GCS: {e}")
        raise

# ISSUE #5: Functions that fetch data and publish events to Pub/Sub
def fetch_and_publish_mbta_routes():
    """
    Fetch MBTA routes data and publish to Pub/Sub - ISSUE #5: Asynchronous pattern
    This demonstrates the async pattern: Fetch -> Store -> Publish -> (Transform later)
    """
    try:
        # Fetch data
        data = get_mbta_routes()
        
        # Save to GCS
        gcs_uri = save_to_gcs(data, 'mbta', 'routes')
        
        # ISSUE #5: Publish event instead of processing inline
        publish_transit_data_event('mbta', 'routes', gcs_uri)
        
        return gcs_uri
    
    except Exception as e:
        logger.error(f"Error in fetch_and_publish_mbta_routes: {e}")
        raise

def fetch_and_publish_mbta_predictions(route_ids):
    """Fetch MBTA predictions data and publish to Pub/Sub - ISSUE #5: Asynchronous pattern"""
    try:
        # Fetch data
        data = get_mbta_predictions(route_ids=route_ids)
        
        # Save to GCS
        gcs_uri = save_to_gcs(data, 'mbta', 'predictions')
        
        # ISSUE #5: Publish event instead of processing inline
        publish_transit_data_event('mbta', 'predictions', gcs_uri)
        
        return gcs_uri
    
    except Exception as e:
        logger.error(f"Error in fetch_and_publish_mbta_predictions: {e}")
        raise

def fetch_and_publish_wmata_rail_predictions():
    """Fetch WMATA rail predictions data and publish to Pub/Sub - ISSUE #5: Asynchronous pattern"""
    try:
        # Fetch data
        data = get_wmata_rail_predictions()
        
        # Save to GCS
        gcs_uri = save_to_gcs(data, 'wmata', 'rail_predictions')
        
        # ISSUE #5: Publish event instead of processing inline
        publish_transit_data_event('wmata', 'rail_predictions', gcs_uri)
        
        return gcs_uri
    
    except Exception as e:
        logger.error(f"Error in fetch_and_publish_wmata_rail_predictions: {e}")
        raise

def fetch_and_publish_cta_train_positions(route_ids):
    """Fetch CTA train positions data and publish to Pub/Sub - ISSUE #5: Asynchronous pattern"""
    try:
        # Fetch data
        data = get_cta_train_positions(route_ids=route_ids)
        
        # Save to GCS
        gcs_uri = save_to_gcs(data, 'cta', 'train_positions')
        
        # ISSUE #5: Publish event instead of processing inline
        publish_transit_data_event('cta', 'train_positions', gcs_uri)
        
        return gcs_uri
    
    except Exception as e:
        logger.error(f"Error in fetch_and_publish_cta_train_positions: {e}")
        raise

def fetch_all_transit_data():
    """
    Fetch all transit data from all sources and publish to Pub/Sub
    ISSUE #5: This is the main entry point for the asynchronous data ingest process
    """
    results = {}
    
    try:
        # Fetch MBTA data
        logger.info("Fetching MBTA data...")
        results['mbta_routes'] = fetch_and_publish_mbta_routes()
        
        # Fetch MBTA predictions for specific routes
        target_routes = ['Red', 'Green-B', 'Green-C', 'Green-D', 'Green-E']
        logger.info(f"Fetching MBTA predictions for routes: {target_routes}")
        results['mbta_predictions'] = fetch_and_publish_mbta_predictions(target_routes)
        
        # Fetch WMATA data
        logger.info("Fetching WMATA rail predictions...")
        results['wmata_rail_predictions'] = fetch_and_publish_wmata_rail_predictions()
        
        # Fetch CTA data
        target_routes = ['Red', 'Blue', 'Brn', 'G', 'Org', 'P', 'Pink', 'Y']
        logger.info(f"Fetching CTA train positions for routes: {target_routes}")
        results['cta_train_positions'] = fetch_and_publish_cta_train_positions(target_routes)
        
        logger.info("Successfully fetched and published all transit data")
        return results
    
    except Exception as e:
        logger.error(f"Error in fetch_all_transit_data: {e}")
        return results
