"""
Script to set up BigQuery dataset and tables.
"""

import os
import argparse
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_bigquery(project_id, dataset_id="sports_analytics", region="us-central1"):
    """
    Set up BigQuery dataset and tables.

    Args:
        project_id (str): Google Cloud Project ID
        dataset_id (str): BigQuery dataset ID
        region (str): Google Cloud region
    """
    # Initialize client
    client = bigquery.Client(project=project_id)

    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = region

    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {project_id}.{dataset_id} created or already exists")
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        return

    # Read SQL files and execute them
    sql_dir = "sql/create_tables"
    if os.path.exists(sql_dir):
        for sql_file in sorted(os.listdir(sql_dir)):
            if sql_file.endswith(".sql"):
                with open(os.path.join(sql_dir, sql_file), "r") as f:
                    sql = f.read()

                # Replace dataset name if needed
                sql = sql.replace("sports_analytics", dataset_id)

                try:
                    logger.info(f"Executing SQL from {sql_file}...")
                    query_job = client.query(sql)
                    query_job.result()  # Wait for the query to complete
                    logger.info(f"Successfully executed {sql_file}")
                except Exception as e:
                    logger.error(f"Error executing SQL from {sql_file}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="Set up BigQuery dataset and tables")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--dataset-id", default="sports_analytics", help="BigQuery dataset ID")
    parser.add_argument("--region", default="us-central1", help="Google Cloud region")

    args = parser.parse_args()
    setup_bigquery(args.project_id, args.dataset_id, args.region)


if __name__ == "__main__":
    main()