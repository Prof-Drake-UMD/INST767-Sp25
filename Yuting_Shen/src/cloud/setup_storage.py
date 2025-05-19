"""
Script to set up Google Cloud Storage buckets for the pipeline.
"""

import os
import argparse
import logging
from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_buckets(project_id, region="us-central1"):
    """
    Create the necessary storage buckets for the pipeline.

    Args:
        project_id (str): Google Cloud Project ID
        region (str): Google Cloud region
    """
    storage_client = storage.Client(project=project_id)

    # Create buckets for different stages of the pipeline
    buckets_to_create = [
        f"{project_id}-raw-data",
        f"{project_id}-processed-data",
        f"{project_id}-functions-code"
    ]

    for bucket_name in buckets_to_create:
        try:
            if storage_client.lookup_bucket(bucket_name):
                logger.info(f"Bucket {bucket_name} already exists")
            else:
                bucket = storage_client.create_bucket(bucket_name, location=region)
                logger.info(f"Created bucket {bucket.name} in {bucket.location}")
        except Exception as e:
            logger.error(f"Error creating bucket {bucket_name}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="Set up GCS buckets")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--region", default="us-central1", help="Google Cloud region")

    args = parser.parse_args()
    create_buckets(args.project_id, args.region)


if __name__ == "__main__":
    main()