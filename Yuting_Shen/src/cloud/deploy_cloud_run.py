"""
Script to deploy the transformation service to Google Cloud Run.
"""

import os
import argparse
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def deploy_transform_service(project_id, region="us-central1"):
    """
    Deploy the data transformation service to Cloud Run.

    Args:
        project_id (str): Google Cloud Project ID
        region (str): Google Cloud region
    """
    # Service name
    service_name = "sports-data-transform"

    # Build the container image
    image_name = f"gcr.io/{project_id}/{service_name}"
    build_cmd = f"gcloud builds submit --tag {image_name} ."

    logger.info(f"Building container image {image_name}...")
    build_result = subprocess.run(build_cmd, shell=True)

    if build_result.returncode != 0:
        logger.error("Failed to build container image")
        return

    # Deploy to Cloud Run
    deploy_cmd = f"""
    gcloud run deploy {service_name} \
      --project={project_id} \
      --region={region} \
      --image={image_name} \
      --memory=1Gi \
      --timeout=15m \
      --cpu=1 \
      --allow-unauthenticated
    """

    logger.info(f"Deploying service {service_name} to Cloud Run...")
    deploy_result = subprocess.run(deploy_cmd, shell=True)

    if deploy_result.returncode == 0:
        logger.info(f"Successfully deployed {service_name}")
        # Get the service URL
        url_cmd = f"gcloud run services describe {service_name} --region={region} --format='value(status.url)'"
        url_result = subprocess.run(url_cmd, shell=True, capture_output=True, text=True)
        service_url = url_result.stdout.strip()
        logger.info(f"Service URL: {service_url}")
    else:
        logger.error(f"Failed to deploy {service_name}")


def main():
    parser = argparse.ArgumentParser(description="Deploy transformation service to Cloud Run")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--region", default="us-central1", help="Google Cloud region")

    args = parser.parse_args()
    deploy_transform_service(args.project_id, args.region)


if __name__ == "__main__":
    main()