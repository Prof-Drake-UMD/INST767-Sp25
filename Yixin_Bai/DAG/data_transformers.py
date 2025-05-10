# data_transformers.py
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import json
from google.cloud import bigquery
from google.cloud import storage
from config import GCP_CONFIG, BQ_CONFIG, GCS_CONFIG

# Configure logging
logger = logging.getLogger(__name__)
# Ensure logger has handler
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Transformation Functions ---

def transform_mbta_routes(routes_data):
    """Transform MBTA routes data"""
    logger.info("Transforming MBTA routes data")
    routes = routes_data.get('data', [])
    rows = []
    for route in routes:
        route_id = route.get('id', '')
        attributes = route.get('attributes', {})
        rows.append({
            'source': 'mbta',
            'route_id': route_id,
            'route_type': attributes.get('type', None),  # Use None for missing int/float
            'route_name': attributes.get('long_name', ''),
            'route_short_name': attributes.get('short_name', ''),
            'route_color': attributes.get('color', ''),
            'route_text_color': attributes.get('text_color', ''),
            'route_description': attributes.get('description', ''),
            'timestamp': datetime.now().isoformat()  # Consider UTC: datetime.utcnow()
        })
    return pd.DataFrame(rows)

def transform_mbta_predictions(predictions_data):
    """Transform MBTA predictions data"""
    logger.info("Transforming MBTA predictions data")
    predictions = predictions_data.get('data', [])
    rows = []
    for prediction in predictions:
        prediction_id = prediction.get('id', '')
        attributes = prediction.get('attributes', {})
        relationships = prediction.get('relationships', {})
        route_id = relationships.get('route', {}).get('data', {}).get('id', '')
        stop_id = relationships.get('stop', {}).get('data', {}).get('id', '')
        vehicle_id = relationships.get('vehicle', {}).get('data', {}).get('id', '')
        rows.append({
            'source': 'mbta',
            'prediction_id': prediction_id,
            'route_id': route_id,
            'stop_id': stop_id,
            'vehicle_id': vehicle_id,
            'direction_id': attributes.get('direction_id', None),
            'departure_time': attributes.get('departure_time', None),  # Already ISO format
            'arrival_time': attributes.get('arrival_time', None),  # Already ISO format
            'status': attributes.get('status', ''),
            'timestamp': datetime.now().isoformat()
        })
    return pd.DataFrame(rows)

def transform_wmata_rail_predictions(predictions_data):
    """Transform WMATA rail predictions data"""
    logger.info("Transforming WMATA rail predictions data")
    predictions = predictions_data.get('Trains', [])
    rows = []
    current_time_iso = datetime.now().isoformat()  # Record timestamp once per batch
    for prediction in predictions:
        # Handle minutes ('ARR', 'BRD', number) - calculate arrival time if possible
        arrival_time_calc = None
        minutes_str = prediction.get('Min', '')
        if minutes_str == 'ARR' or minutes_str == 'BRD':
            # Representing immediate arrival/boarding
            arrival_time_calc = current_time_iso
        elif minutes_str.isdigit():
            try:
                minutes_int = int(minutes_str)
                # This calculation is approximate; using server timestamp is better
                # For simplicity, we'll store minutes and let SQL handle calc later
                pass  # Keep minutes_str as is for now
            except ValueError:
                minutes_str = ''  # Clear if not digit, ARR or BRD
        else:
            minutes_str = ''  # Clear unexpected values

        rows.append({
            'source': 'wmata',
            'prediction_id': f"{prediction.get('LocationCode', '')}-{prediction.get('Car', '')}-{prediction.get('DestinationCode', '')}-{prediction.get('Min', '')}",  # Make ID more unique
            'station_code': prediction.get('LocationCode', ''),
            'station_name': prediction.get('LocationName', ''),
            'train_line': prediction.get('Line', ''),
            'destination_code': prediction.get('DestinationCode', ''),
            'destination_name': prediction.get('DestinationName', ''),
            'minutes_to_arrival': minutes_str,  # Store the original string
            'cars': prediction.get('Car', ''),
            'timestamp': current_time_iso  # Use consistent timestamp
        })
    return pd.DataFrame(rows)

def transform_cta_train_arrivals(arrivals_data):
    """Transform CTA train arrivals data"""
    logger.info("Transforming CTA train arrivals data")

    # Check for error structure first if API helper returns dict on non-json
    if isinstance(arrivals_data, dict) and arrivals_data.get('ctatt', {}).get('errCd'):
        err_code = arrivals_data['ctatt']['errCd']
        err_msg = arrivals_data['ctatt'].get('errNm', 'Unknown CTA API Error')
        logger.error(f"CTA API returned error code {err_code}: {err_msg}")
        # Return empty DataFrame or specific error structure
        return pd.DataFrame()

    # Proceed if it looks like valid data structure
    arrivals = arrivals_data.get('ctatt', {}).get('eta', [])
    if not isinstance(arrivals, list):  # Handle single arrival case
        arrivals = [arrivals] if arrivals else []

    rows = []
    current_time_iso = datetime.now().isoformat()
    for arrival in arrivals:
        # CTA times appear to be in 'YYYYMMDD HH:MM:SS' format, keep as strings for now
        rows.append({
            'source': 'cta',
            'station_id': arrival.get('staId', ''),
            'station_name': arrival.get('staNm', ''),
            'train_run_number': arrival.get('rn', ''),
            'route_id': arrival.get('rt', ''),
            'destination_name': arrival.get('destNm', ''),
            'direction': arrival.get('trDr', ''),
            'prediction_generated_time': arrival.get('prdt', ''),  # Keep original format string
            'arrival_time': arrival.get('arrT', ''),  # Keep original format string
            'is_approaching': arrival.get('isApp', ''),
            'is_scheduled': arrival.get('isSch', ''),
            'is_fault': arrival.get('isFlt', ''),  # Added potential field
            'is_delayed': arrival.get('isDly', ''),  # Added potential field
            'flags': arrival.get('flags', ''),      # Added potential field
            'timestamp': current_time_iso
        })
    return pd.DataFrame(rows)

# --- GCS and BigQuery Functions ---

def save_to_gcs(df, source, data_type):
    """Save a DataFrame to Google Cloud Storage as newline-delimited JSON."""
    # Check if DataFrame is empty before proceeding
    if df.empty:
        logger.warning(f"DataFrame for {source} {data_type} is empty. Skipping GCS save.")
        return None  # Return None to indicate nothing was saved

    logger.info(f"Saving {source} {data_type} data to GCS")
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"{source}_{data_type}_{timestamp}.json"
    gcs_object_name = f"{GCS_CONFIG['temp_dir']}/{filename}"
    gcs_path = f"gs://{GCS_CONFIG['bucket_name']}/{gcs_object_name}"

    try:
        # Convert to newline-delimited JSON string
        json_data = df.to_json(orient='records', lines=True, date_format='iso')  # Use ISO date format

        storage_client = storage.Client(project=GCP_CONFIG['project_id'])
        bucket = storage_client.bucket(GCS_CONFIG['bucket_name'])  # Use .bucket()
        blob = bucket.blob(gcs_object_name)

        logger.info(f"Uploading to {gcs_path}")
        blob.upload_from_string(json_data, content_type='application/json')
        logger.info(f"Successfully uploaded {source} {data_type} data to {gcs_path}")
        return gcs_path

    except Exception as e:
        logger.error(f"Failed to save {source} {data_type} to GCS at {gcs_path}: {e}")
        raise  # Re-raise error to fail the Airflow task

def load_to_bigquery(gcs_path, table_id):
    """Load data from a GCS JSON file to BigQuery."""
    # If gcs_path is None (because save_to_gcs was skipped), skip BQ load
    if gcs_path is None:
        logger.info(f"No GCS path provided for table {table_id}, likely due to empty source data. Skipping BigQuery load.")
        return True  # Return True indicating successful (non-)operation for this step

    logger.info(f"Loading data from {gcs_path} to BigQuery table {table_id}")
    bq_client = bigquery.Client(project=GCP_CONFIG['project_id'])
    full_table_id = f"{GCP_CONFIG['project_id']}.{BQ_CONFIG['dataset_id']}.{table_id}"

    # Configure the load job with schema update options
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION  # Allow STRING -> TIMESTAMP etc.
        ],
        # Consider adding time partitioning if tables grow large
        # time_partitioning=bigquery.TimePartitioning(
        #     type_=bigquery.TimePartitioningType.DAY,
        #     field="timestamp"  # Partition by the recorded timestamp
        # )
    )

    try:
        # Start the load job
        load_job = bq_client.load_table_from_uri(
            gcs_path, full_table_id, job_config=job_config
        )
        logger.info(f"Started BigQuery load job {load_job.job_id} from {gcs_path} to {full_table_id}")

        # Wait for the job to complete
        load_job.result()  # Raises GoogleAPICallError on job failure
        logger.info(f"BigQuery load job {load_job.job_id} completed successfully.")

        # Check for errors reported by the job (optional but good practice)
        if load_job.errors:
            logger.error(f"BigQuery load job {load_job.job_id} finished with errors:")
            for error in load_job.errors:
                logger.error(f" - {error['reason']}: {error['message']}")
            # Decide if errors constitute failure
            # raise RuntimeError(f"BigQuery load job {load_job.job_id} had errors.")
        else:
            destination_table = bq_client.get_table(full_table_id)
            logger.info(f"Loaded {destination_table.num_rows} rows into {full_table_id}")

        return True

    except Exception as e:
        logger.error(f"BigQuery load job failed for {full_table_id} from {gcs_path}: {e}")
        raise  # Re-raise error to fail the Airflow task

def transform_cta_train_positions(positions_data):
    """Transform CTA train positions data"""
    logger.info("Transforming CTA train positions data")
    rows = []
    current_time_iso = datetime.now().isoformat()

    # Check response structure and errors
    if isinstance(positions_data, dict) and 'ctatt' in positions_data:
        ctatt_data = positions_data.get('ctatt', {})
        err_code = ctatt_data.get('errCd')
        err_msg = ctatt_data.get('errNm')

        if err_code is not None and err_code != '0':
            logger.error(f"CTA Positions API returned error code {err_code}: {err_msg}")
            return pd.DataFrame() # Return empty on error

        route_list = ctatt_data.get('route', [])
        if not isinstance(route_list, list): # Handle single route case
             route_list = [route_list] if route_list else []

        for route_data in route_list:
            # Route name might be attribute like '@name' or element text depending on JSON conversion
            route_name_attr = route_data.get('@name', '') # Check attribute first
            if not route_name_attr and isinstance(route_data.get('name'), str) : # Check element text
                 route_name_attr = route_data.get('name')

            train_list = route_data.get('train', [])
            if not isinstance(train_list, list): # Handle single train case
                 train_list = [train_list] if train_list else []

            for train in train_list:
                # Extract relevant fields from the 'train' object
                row = {
                    'source': 'cta',
                    'route_id': route_name_attr, # Route identifier (e.g., 'red')
                    'vehicle_id': train.get('rn', ''), # Run number as vehicle ID
                    'destination_name': train.get('destNm', ''),
                    'direction': train.get('trDr', ''),
                    'next_station_id': train.get('nextStaId', ''),
                    'next_station_name': train.get('nextStaNm', ''),
                    'predicted_arrival_time': train.get('arrT', ''), # Predicted arrival at next station
                    'is_approaching': train.get('isApp', ''),
                    'is_delayed': train.get('isDly', ''),
                    'latitude': train.get('lat'),
                    'longitude': train.get('lon'),
                    'heading': train.get('heading'),
                    'timestamp': current_time_iso # Recording time
                }
                rows.append(row)
    elif isinstance(positions_data, str): # Handle non-JSON case from api_helpers
         logger.warning(f"Received non-dict/non-JSON response from CTA API helper: {positions_data[:100]}")
         return pd.DataFrame()
    else: # Handle other unexpected types
         logger.warning(f"Received unexpected data type from CTA Positions API helper: {type(positions_data)}")
         return pd.DataFrame()

    return pd.DataFrame(rows)

# --- Ensure save_to_gcs and load_to_bigquery have the empty checks ---

def load_to_bigquery(gcs_path, table_id):
    """Load data from a GCS JSON file to BigQuery."""
    # If gcs_path is None (because save_to_gcs was skipped), skip BQ load
    if gcs_path is None:
        logger.info(f"No GCS path provided for table {table_id}, likely due to empty source data. Skipping BigQuery load.")
        return True # Indicate successful skip

    # ... (rest of the function including LoadJobConfig with schema_update_options as provided before) ...
    return True

def integrate_transit_data():
    """Query and integrate data from source tables into the integrated table."""
    logger.info("Integrating transit data from all sources into final table")
    bq_client = bigquery.Client(project=GCP_CONFIG['project_id'])

    # Construct table references safely
    project = GCP_CONFIG['project_id']
    dataset = BQ_CONFIG['dataset_id']
    mbta_table = f"`{project}.{dataset}.{BQ_CONFIG['tables']['mbta']}`"
    wmata_table = f"`{project}.{dataset}.{BQ_CONFIG['tables']['wmata']}`"
    cta_table = f"`{project}.{dataset}.{BQ_CONFIG['tables']['cta']}`"
    cta_positions_table = f"`{project}.{dataset}.{BQ_CONFIG['tables']['cta_positions']}`" 
    integrated_table = f"`{project}.{dataset}.{BQ_CONFIG['tables']['integrated']}`"

    integrated_query = f"""
    CREATE OR REPLACE TABLE {integrated_table}
    PARTITION BY DATE(recorded_at) 
    CLUSTER BY transit_system, route_id 
    AS

    WITH mbta_standardized AS (

      SELECT
        'mbta' AS transit_system,
        CAST(route_id AS STRING) AS route_id,
        CAST(stop_id AS STRING) AS station_id,
        CAST(NULL AS STRING) AS station_name,
        CAST(vehicle_id AS STRING) AS vehicle_id,
        CAST(direction_id AS INT64) AS direction_id,
        CAST(arrival_time AS TIMESTAMP) AS arrival_time, 
        CAST(departure_time AS TIMESTAMP) AS departure_time,
        CAST(status AS STRING) AS status,
        CAST(prediction_id AS STRING) AS prediction_id,
        CAST(NULL AS FLOAT64) AS latitude,
        CAST(NULL AS FLOAT64) AS longitude,
        CAST(NULL AS STRING) AS heading,
        'prediction' AS data_type,  
        CAST(timestamp AS TIMESTAMP) AS recorded_at
      FROM {mbta_table}
      WHERE source = 'mbta'
        AND prediction_id IS NOT NULL
    ),

    wmata_standardized AS (
    
      SELECT
        'wmata' AS transit_system,
        CAST(train_line AS STRING) AS route_id,
        CAST(station_code AS STRING) AS station_id,
        CAST(station_name AS STRING) AS station_name,
        CAST(NULL AS STRING) AS vehicle_id,
        CAST(NULL AS INT64) AS direction_id,
        CASE
            WHEN minutes_to_arrival = 'ARR' THEN timestamp
            WHEN minutes_to_arrival = 'BRD' THEN timestamp
            WHEN SAFE_CAST(minutes_to_arrival AS INT64) IS NOT NULL
                THEN TIMESTAMP_ADD(timestamp, INTERVAL SAFE_CAST(minutes_to_arrival AS INT64) MINUTE)
            ELSE NULL
        END AS arrival_time,
        CAST(NULL AS TIMESTAMP) AS departure_time,
        CAST(NULL AS STRING) AS status,
        CAST(prediction_id AS STRING) AS prediction_id,
        CAST(NULL AS FLOAT64) AS latitude,
        CAST(NULL AS FLOAT64) AS longitude,
        CAST(NULL AS STRING) AS heading,
        'prediction' AS data_type,  
        CAST(timestamp AS TIMESTAMP) AS recorded_at
      FROM {wmata_table}
      WHERE source = 'wmata'
    ),

    cta_predictions_standardized AS (
    
      SELECT
        'cta' AS transit_system,
        CAST(route_id AS STRING) AS route_id,
        CAST(station_id AS STRING) AS station_id,
        CAST(station_name AS STRING) AS station_name,
        CAST(train_run_number AS STRING) AS vehicle_id,
        CAST(direction AS INT64) AS direction_id,
        CAST(arrival_time AS TIMESTAMP) AS arrival_time,
        CAST(NULL AS TIMESTAMP) AS departure_time,
        CONCAT(
            IF(is_approaching = '1', 'Approaching;', ''),
            IF(is_scheduled = '1', 'Scheduled;', '')
        
            
        ) AS status,
        CAST(NULL AS STRING) AS prediction_id,
        CAST(NULL AS FLOAT64) AS latitude,
        CAST(NULL AS FLOAT64) AS longitude,
        CAST(NULL AS STRING) AS heading,
        'prediction' AS data_type,  
        CAST(timestamp AS TIMESTAMP) AS recorded_at
      FROM {cta_table}
      WHERE source = 'cta'
    ),
    
    cta_positions_standardized AS (

      SELECT
        'cta' AS transit_system,
        CAST(route_id AS STRING) AS route_id,
        CAST(next_station_id AS STRING) AS station_id,  
        CAST(next_station_name AS STRING) AS station_name,  
        CAST(vehicle_id AS STRING) AS vehicle_id,
        CAST(direction AS INT64) AS direction_id,
        CAST(predicted_arrival_time AS TIMESTAMP) AS arrival_time,  
        CAST(NULL AS TIMESTAMP) AS departure_time,
        CONCAT(
            IF(is_approaching = '1', 'Approaching;', ''),
            IF(is_delayed = '1', 'Delayed;', '')
        ) AS status,
        CAST(NULL AS STRING) AS prediction_id,
        CAST(latitude AS FLOAT64) AS latitude,  
        CAST(longitude AS FLOAT64) AS longitude,  
        CAST(heading AS STRING) AS heading, 
        'position' AS data_type,  
        CAST(timestamp AS TIMESTAMP) AS recorded_at
      FROM {cta_positions_table}
      WHERE source = 'cta'
    )


    SELECT * FROM mbta_standardized
    UNION ALL
    SELECT * FROM wmata_standardized
    UNION ALL
    SELECT * FROM cta_predictions_standardized
    UNION ALL
    SELECT * FROM cta_positions_standardized;
    """

    try:
        logger.info(f"Executing integration query to create/replace table: {integrated_table}")
        query_job = bq_client.query(integrated_query)
        query_job.result()  
        logger.info(f"Successfully created/replaced integrated table: {integrated_table}")
        return True
    except Exception as e:
        logger.error(f"Failed to execute integration query: {e}")
        raise