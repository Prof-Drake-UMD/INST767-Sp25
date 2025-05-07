# transit_data_dag.py
import os
from datetime import datetime, timedelta
import logging 
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.exceptions import AirflowSkipException

from config import BQ_CONFIG, GCS_CONFIG, DATA_REFRESH, GCP_CONFIG
import api_helpers
import data_transformers

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 29),  # Yesterday
}

# Define the DAG
dag = DAG(
    'transit_data_integration',
    default_args=default_args,
    description='A DAG to integrate transit data from MBTA, WMATA, and CTA',
    schedule_interval=DATA_REFRESH['schedule_interval'],
    catchup=False,
    tags=['transit', 'integration'],
)

# Create GCS bucket if it doesn't exist
create_gcs_bucket = GCSCreateBucketOperator(
    task_id='create_gcs_bucket',
    bucket_name=GCS_CONFIG['bucket_name'],
    project_id=GCP_CONFIG['project_id'],
    location=GCP_CONFIG['region'],
    storage_class='STANDARD',
    dag=dag,
)

# Create BigQuery dataset if it doesn't exist
create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bq_dataset',
    dataset_id=BQ_CONFIG['dataset_id'],
    project_id=GCP_CONFIG['project_id'],
    location=GCP_CONFIG['region'],
    dag=dag,
)

# MBTA Data Pipeline Tasks
def fetch_transform_load_mbta_routes():
    # Fetch data
    routes_data = api_helpers.get_mbta_routes()
    
    # Transform data
    transformed_data = data_transformers.transform_mbta_routes(routes_data)
    
    # Save to GCS
    gcs_path = data_transformers.save_to_gcs(transformed_data, 'mbta', 'routes')
    
    # Load to BigQuery
    data_transformers.load_to_bigquery(gcs_path, BQ_CONFIG['tables']['mbta'])

def fetch_transform_load_mbta_predictions():
    target_routes = ['Red', 'Green-B', 'Green-C', 'Green-D', 'Green-E']

    try:
        logger.info(f"Fetching MBTA predictions for routes: {', '.join(target_routes)}")
        predictions_data = api_helpers.get_mbta_predictions(route_ids=target_routes)

    except ValueError as e:
        logger.error(f"ValueError fetching MBTA predictions: {e}")
        # from airflow.exceptions import AirflowSkipException
        # raise AirflowSkipException(f"Skipping task due to fetch error: {e}")
        return 
    except Exception as e: 
        logger.error(f"Error fetching MBTA predictions: {e}")
        raise e 

    if not predictions_data or not predictions_data.get('data'):
        logger.warning(f"No MBTA prediction data received for routes: {', '.join(target_routes)}.")
        # from airflow.exceptions import AirflowSkipException
        # raise AirflowSkipException("No prediction data received.")
        return 

    logger.info(f"Received {len(predictions_data.get('data', []))} prediction records for specified routes.")

    # Transform data
    transformed_data = data_transformers.transform_mbta_predictions(predictions_data)

    if transformed_data.empty:
         logger.warning("Transformed prediction data is empty.")
         return # or raise AirflowSkipException

    # Save to GCS
    gcs_path = data_transformers.save_to_gcs(transformed_data, 'mbta', 'predictions')

    # Load to BigQuery
    data_transformers.load_to_bigquery(gcs_path, BQ_CONFIG['tables']['mbta'])

# WMATA Data Pipeline Tasks
def fetch_transform_load_wmata_rail_predictions():
    # Fetch data
    predictions_data = api_helpers.get_wmata_rail_predictions()
    
    # Transform data
    transformed_data = data_transformers.transform_wmata_rail_predictions(predictions_data)
    
    # Save to GCS
    gcs_path = data_transformers.save_to_gcs(transformed_data, 'wmata', 'rail_predictions')
    
    # Load to BigQuery
    data_transformers.load_to_bigquery(gcs_path, BQ_CONFIG['tables']['wmata'])

# CTA Data Pipeline Tasks
def fetch_transform_load_cta_train_arrivals():
    # !!! IMPORTANT: Multiple station IDs as a list of CTA station mapids !!!
    station_ids_to_query = ['40380', '41450', '41320', '40890']  # Example: Clark/Lake, Fullerton, Belmont, State/Lake
    station_query_param = ','.join(station_ids_to_query)
    
    try:
        logger.info(f"Fetching CTA train arrivals for station mapids: {station_query_param}")
        arrivals_data = api_helpers.get_cta_train_arrivals(station_id=station_query_param)

        # Transform handles potential non-JSON response internally now, might return empty df
        transformed_data = data_transformers.transform_cta_train_arrivals(arrivals_data)

        if transformed_data.empty:
            logger.info(f"No CTA train arrivals data returned or transformed for stations: {station_query_param}. Skipping GCS/BQ steps.")
            # Using AirflowSkipException to mark task as Skipped in UI
            raise AirflowSkipException(f"No CTA data to load for stations: {station_query_param}")

        logger.info(f"Transformed CTA data has {len(transformed_data)} rows. Proceeding to save.")
        gcs_path = data_transformers.save_to_gcs(transformed_data, 'cta', 'train_arrivals')
        data_transformers.load_to_bigquery(gcs_path, BQ_CONFIG['tables']['cta'])
        logger.info("CTA train arrivals task finished successfully.")
        
    except AirflowSkipException as e:  # <--- Specifically catch Skip exception
        logger.info(f"Skipping CTA task: {e}")
        raise  # <--- Must re-raise Skip exception to show as Skipped in Airflow UI
    except ValueError as ve:  # Catch potential non-JSON error from helper
        logger.error(f"CTA train arrivals task failed processing API response: {ve}")
        raise
    except Exception as e:
        logger.error(f"CTA train arrivals task failed: {e}")
        raise

# NEW function for CTA Train Positions
def fetch_transform_load_cta_train_positions():
    # Define the routes to query for positions
    # Route codes: Red, Blue, Brn, G, Org, P, Pink, Y
    target_routes = ['Red', 'Blue', 'Brn', 'G', 'Org', 'P', 'Pink', 'Y']
    
    try:
        logger.info(f"Starting CTA train positions task for routes: {', '.join(target_routes)}")
        positions_data = api_helpers.get_cta_train_positions(route_ids=target_routes)

        # Transform data
        transformed_data = data_transformers.transform_cta_train_positions(positions_data)

        if transformed_data.empty:
            logger.info(f"No CTA train position data returned or transformed for routes {', '.join(target_routes)}. Skipping GCS/BQ steps.")
            raise AirflowSkipException(f"No CTA position data to load for routes {', '.join(target_routes)}.")

        logger.info(f"Transformed CTA position data has {len(transformed_data)} rows. Proceeding to save.")
        gcs_path = data_transformers.save_to_gcs(transformed_data, 'cta', 'train_positions')
        
        # Load to BigQuery
        data_transformers.load_to_bigquery(gcs_path, BQ_CONFIG['tables']['cta_positions'])
        logger.info("CTA train positions task finished successfully with data.")
        
    except AirflowSkipException as e:
        logger.info(f"Skipping CTA positions task: {e}")
        raise
    except ValueError as ve:
        logger.error(f"CTA positions task failed due to configuration/API response: {ve}")
        raise
    except Exception as e:
        logger.error(f"CTA positions task failed with unexpected error: {e}")
        raise

# Data Integration Task
def integrate_data():
    data_transformers.integrate_transit_data()

# Define the operators for MBTA data
fetch_mbta_routes_task = PythonOperator(
    task_id='fetch_mbta_routes',
    python_callable=fetch_transform_load_mbta_routes,
    dag=dag,
)

fetch_mbta_predictions_task = PythonOperator(
    task_id='fetch_mbta_predictions',
    python_callable=fetch_transform_load_mbta_predictions,
    dag=dag,
)

# Define the operators for WMATA data
fetch_wmata_rail_predictions_task = PythonOperator(
    task_id='fetch_wmata_rail_predictions',
    python_callable=fetch_transform_load_wmata_rail_predictions,
    dag=dag,
)

# Define the operators for CTA data - MODIFIED task_id and function
fetch_cta_train_positions_task = PythonOperator(
    task_id='fetch_cta_train_positions',
    python_callable=fetch_transform_load_cta_train_positions,
    dag=dag,
)

# Define the operator for data integration
integrate_transit_data_task = PythonOperator(
    task_id='integrate_transit_data',
    python_callable=integrate_data,
    dag=dag,
)

# Define the task dependencies
create_gcs_bucket >> create_bq_dataset
create_bq_dataset >> [fetch_mbta_routes_task, fetch_mbta_predictions_task, fetch_wmata_rail_predictions_task, fetch_cta_train_positions_task]
[fetch_mbta_routes_task, fetch_mbta_predictions_task, fetch_wmata_rail_predictions_task, fetch_cta_train_positions_task] >> integrate_transit_data_task