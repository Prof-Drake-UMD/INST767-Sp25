#!/usr/bin/python
# main.py
"""
# ISSUE #6: MAIN ENTRY POINT FOR TRANSIT DATA INTEGRATION PIPELINE
Main script for the Transit Data Integration Pipeline.
This script provides a command-line interface to run the different components
of the pipeline either locally or in the cloud.
"""
import os
import sys
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"transit_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ISSUE #3: Function to run the local pipeline
def run_local_pipeline(args):
    """Run the pipeline locally - ISSUE #3: Local testing"""
    logger.info("Running local pipeline")
    
    # Import local modules
    from local_deployment.local_deployment import main as local_main
    
    # Run local pipeline
    local_main()

# ISSUE #4: Function to deploy GCP resources
def deploy_gcp_resources(args):
    """Deploy resources to Google Cloud Platform - ISSUE #4: GCP deployment"""
    logger.info("Deploying GCP resources")
    
    # Validate environment
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set. Please set it to your service account key file.")
        return 1
    
    # Run the deployment script
    import subprocess
    script_path = os.path.join('google_cloud', 'deploy_gcp_resources.sh')
    
    # Make the script executable
    os.chmod(script_path, 0o755)
    
    # Execute the script
    result = subprocess.run([script_path], check=True)
    
    return result.returncode

# ISSUE #5: Function to deploy the subscriber service
def deploy_subscriber_service(args):
    """Deploy the subscriber service to Cloud Run - ISSUE #5: Deploying subscriber service"""
    logger.info("Deploying subscriber service")
    
    # Validate environment
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set. Please set it to your service account key file.")
        return 1
    
    # Run the deployment script
    import subprocess
    script_path = os.path.join('google_cloud', 'deploy_subscriber_service.sh')
    
    # Make the script executable
    os.chmod(script_path, 0o755)
    
    # Execute the script
    result = subprocess.run([script_path], check=True)
    
    return result.returncode

# ISSUE #5: Function to run the ingest step in the cloud
def run_cloud_ingest(args):
    """Run the ingest step in the cloud - ISSUE #5: Running async ingest"""
    logger.info("Running cloud ingest")
    
    # Import cloud modules
    from google_cloud.modified_api_helpers import fetch_all_transit_data
    
    # Run ingest - this will fetch data and publish to Pub/Sub
    results = fetch_all_transit_data()
    
    # Print results
    for source, uri in results.items():
        logger.info(f"{source}: {uri}")
    
    return 0

# ISSUE #6: Function to create BigQuery tables
def create_bigquery_tables(args):
    """Create BigQuery tables using the schema definitions - ISSUE #6: BigQuery setup"""
    logger.info("Creating BigQuery tables")
    
    # Import Google Cloud libraries
    from google.cloud import bigquery
    
    # Read the SQL script
    with open('schemas/transit_data_complete.sql', 'r') as f:
        sql = f.read()
    
    # Create BigQuery client
    client = bigquery.Client()
    
    # Execute the SQL script
    job = client.query(sql)
    job.result()  # Wait for the query to finish
    
    logger.info("BigQuery tables created successfully")
    return 0

# ISSUE #6: Function to run the data integration query
def run_integration_query(args):
    """Run the data integration query - ISSUE #6: Data integration"""
    logger.info("Running data integration query")
    
    # Import data transformers
    from data_transformers import integrate_transit_data
    
    # Run the integration
    integrate_transit_data()
    
    logger.info("Data integration completed successfully")
    return 0

def main():
    """Main function - ISSUE #6: Main CLI interface"""
    parser = argparse.ArgumentParser(description='Transit Data Integration Pipeline')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # ISSUE #3: Local pipeline command
    local_parser = subparsers.add_parser('local', help='Run the pipeline locally')
    
    # ISSUE #4: GCP deployment command
    deploy_parser = subparsers.add_parser('deploy', help='Deploy resources to GCP')
    
    # ISSUE #5: Subscriber service deployment command
    subscriber_parser = subparsers.add_parser('deploy-subscriber', help='Deploy the subscriber service')
    
    # ISSUE #5: Cloud ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Run the ingest step in the cloud')
    
    # ISSUE #6: Create BigQuery tables command
    tables_parser = subparsers.add_parser('create-tables', help='Create BigQuery tables')
    
    # ISSUE #6: Integration query command
    integrate_parser = subparsers.add_parser('integrate', help='Run the data integration query')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Execute the appropriate command
    if args.command == 'local':
        return run_local_pipeline(args)
    elif args.command == 'deploy':
        return deploy_gcp_resources(args)
    elif args.command == 'deploy-subscriber':
        return deploy_subscriber_service(args)
    elif args.command == 'ingest':
        return run_cloud_ingest(args)
    elif args.command == 'create-tables':
        return create_bigquery_tables(args)
    elif args.command == 'integrate':
        return run_integration_query(args)
    else:
        parser.print_help()
        return 0

if __name__ == "__main__":
    sys.exit(main())