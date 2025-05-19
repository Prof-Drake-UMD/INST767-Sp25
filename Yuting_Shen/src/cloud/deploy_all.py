"""
Master script to deploy all components to Google Cloud.
"""

import os
import argparse
import logging
import importlib.util
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def import_module_from_file(module_name, file_path):
    """
    Import a module from a file path.

    Args:
        module_name (str): Name to give the module
        file_path (str): Path to the Python file

    Returns:
        module: The imported module
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def deploy_all(project_id, region="us-central1"):
    """
    Deploy all components to Google Cloud.

    Args:
        project_id (str): Google Cloud Project ID
        region (str): Google Cloud region
    """
    # Get the path to the cloud directory
    cloud_dir = os.path.dirname(os.path.abspath(__file__))

    # Step 1: Set up storage buckets
    logger.info("Setting up storage buckets...")
    storage_module = import_module_from_file("setup_storage", os.path.join(cloud_dir, "setup_storage.py"))
    storage_module.create_buckets(project_id, region)

    # Step 2: Set up BigQuery dataset and tables
    logger.info("Setting up BigQuery...")
    bigquery_module = import_module_from_file("setup_bigquery", os.path.join(cloud_dir, "setup_bigquery.py"))
    bigquery_module.setup_bigquery(project_id, "sports_analytics", region)

    # Step 3: Deploy Sports API function
    logger.info("Deploying Sports API function...")
    sports_module = import_module_from_file("deploy_sports_function",
                                            os.path.join(cloud_dir, "deploy_sports_function.py"))
    sports_module.deploy_sports_function(project_id, region)

    # Step 4: Deploy YouTube API function
    logger.info("Deploying YouTube API function...")
    youtube_module = import_module_from_file("deploy_youtube_function",
                                             os.path.join(cloud_dir, "deploy_youtube_function.py"))
    youtube_module.deploy_youtube_function(project_id, region)

    # Step 5: Deploy transformation service to Cloud Run
    logger.info("Deploying transformation service to Cloud Run...")
    cloud_run_module = import_module_from_file("deploy_cloud_run", os.path.join(cloud_dir, "deploy_cloud_run.py"))
    cloud_run_module.deploy_transform_service(project_id, region)

    logger.info("Deployment complete!")


def main():
    parser = argparse.ArgumentParser(description="Deploy all components to Google Cloud")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--region", default="us-central1", help="Google Cloud region")

    args = parser.parse_args()
    deploy_all(args.project_id, args.region)


if __name__ == "__main__":
    main()