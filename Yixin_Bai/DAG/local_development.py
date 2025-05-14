#!/usr/bin/python
# local_deployment.py
"""
# ISSUE #3: LOCAL DEPLOYMENT SCRIPT
Local deployment script for transit data integration.
This script executes the complete data pipeline locally for testing.
"""
import os
import json
import logging
import pandas as pd
from datetime import datetime
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import project modules - ISSUE #3: Using existing modules for local testing
import api_helpers
import data_transformers
from config import MBTA_API, WMATA_API, CTA_API

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("local_deployment.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ISSUE #3: Create output directories for local testing
os.makedirs("./data/raw", exist_ok=True)
os.makedirs("./data/processed", exist_ok=True)
os.makedirs("./data/csv", exist_ok=True)

# ISSUE #3: Functions to fetch and process data from each API source
def fetch_mbta_data():
    """Fetch and process MBTA data - ISSUE #3: Local data collection"""
    logger.info("Fetching MBTA routes data")
    routes_data = api_helpers.get_mbta_routes()
    
    # Save raw data
    with open("./data/raw/mbta_routes.json", "w") as f:
        json.dump(routes_data, f, indent=2)
    
    # Transform routes data
    transformed_routes = data_transformers.transform_mbta_routes(routes_data)
    
    # Fetch predictions for specific routes
    target_routes = ['Red', 'Green-B', 'Green-C', 'Green-D', 'Green-E']
    logger.info(f"Fetching MBTA predictions for routes: {', '.join(target_routes)}")
    
    try:
        predictions_data = api_helpers.get_mbta_predictions(route_ids=target_routes)
        
        # Save raw data
        with open("./data/raw/mbta_predictions.json", "w") as f:
            json.dump(predictions_data, f, indent=2)
        
        # Transform predictions data
        transformed_predictions = data_transformers.transform_mbta_predictions(predictions_data)
        
        # Combine the transformed data
        mbta_data = pd.concat([transformed_routes, transformed_predictions])
        
        # ISSUE #3: Save processed data as CSV for local testing
        mbta_data.to_csv("./data/csv/mbta_data.csv", index=False)
        logger.info(f"Saved MBTA data to CSV with {len(mbta_data)} rows")
        
        return mbta_data
    
    except Exception as e:
        logger.error(f"Error processing MBTA data: {e}")
        return transformed_routes

def fetch_wmata_data():
    """Fetch and process WMATA data - ISSUE #3: Local data collection"""
    logger.info("Fetching WMATA rail predictions data")
    try:
        predictions_data = api_helpers.get_wmata_rail_predictions()
        
        # Save raw data
        with open("./data/raw/wmata_predictions.json", "w") as f:
            json.dump(predictions_data, f, indent=2)
        
        # Transform data
        transformed_data = data_transformers.transform_wmata_rail_predictions(predictions_data)
        
        # ISSUE #3: Save processed data as CSV for local testing
        transformed_data.to_csv("./data/csv/wmata_data.csv", index=False)
        logger.info(f"Saved WMATA data to CSV with {len(transformed_data)} rows")
        
        return transformed_data
    
    except Exception as e:
        logger.error(f"Error processing WMATA data: {e}")
        return pd.DataFrame()

def fetch_cta_data():
    """Fetch and process CTA data - ISSUE #3: Local data collection"""
    try:
        # Fetch CTA train positions
        target_routes = ['Red', 'Blue', 'Brn', 'G', 'Org', 'P', 'Pink', 'Y']
        logger.info(f"Fetching CTA train positions data for routes: {', '.join(target_routes)}")
        positions_data = api_helpers.get_cta_train_positions(route_ids=target_routes)
        
        # Save raw data
        with open("./data/raw/cta_positions.json", "w") as f:
            json.dump(positions_data, f, indent=2)
        
        # Transform data
        transformed_positions = data_transformers.transform_cta_train_positions(positions_data)
        
        # ISSUE #3: Save processed data as CSV for local testing
        transformed_positions.to_csv("./data/csv/cta_positions_data.csv", index=False)
        logger.info(f"Saved CTA positions data to CSV with {len(transformed_positions)} rows")
        
        return transformed_positions
    
    except Exception as e:
        logger.error(f"Error processing CTA data: {e}")
        return pd.DataFrame()

# ISSUE #3: Function to create an integrated dataset for local testing
def create_integrated_dataset(mbta_data, wmata_data, cta_data):
    """Create an integrated dataset from all sources - ISSUE #3: Local data integration"""
    logger.info("Creating integrated dataset")
    
    # Create standardized dataframes with consistent columns
    mbta_standardized = pd.DataFrame()
    if not mbta_data.empty:
        mbta_standardized = mbta_data.assign(
            transit_system='mbta',
            station_name=None,
            direction_id=lambda df: df['direction_id'].astype('Int64'),
            latitude=None,
            longitude=None,
            heading=None,
            data_type='prediction'
        ).rename(columns={
            'timestamp': 'recorded_at'
        })
    
    wmata_standardized = pd.DataFrame()
    if not wmata_data.empty:
        wmata_standardized = wmata_data.assign(
            transit_system='wmata',
            vehicle_id=None,
            direction_id=None,
            departure_time=None,
            status=None,
            latitude=None,
            longitude=None,
            heading=None,
            data_type='prediction'
        ).rename(columns={
            'train_line': 'route_id',
            'station_code': 'station_id',
            'timestamp': 'recorded_at'
        })
        
        # Convert minutes_to_arrival to actual arrival_time
        def calculate_arrival_time(row):
            if row['minutes_to_arrival'] == 'ARR' or row['minutes_to_arrival'] == 'BRD':
                return row['recorded_at']
            try:
                if pd.notna(row['minutes_to_arrival']) and row['minutes_to_arrival'].isdigit():
                    minutes = int(row['minutes_to_arrival'])
                    timestamp = pd.to_datetime(row['recorded_at'])
                    return timestamp + pd.Timedelta(minutes=minutes)
            except:
                pass
            return None
        
        wmata_standardized['arrival_time'] = wmata_standardized.apply(calculate_arrival_time, axis=1)
    
    cta_standardized = pd.DataFrame()
    if not cta_data.empty:
        cta_standardized = cta_data.assign(
            transit_system='cta',
            prediction_id=None,
            departure_time=None,
            data_type='position'
        ).rename(columns={
            'vehicle_id': 'vehicle_id',
            'next_station_id': 'station_id',
            'next_station_name': 'station_name',
            'predicted_arrival_time': 'arrival_time',
            'timestamp': 'recorded_at'
        })
        
        # Create status field from is_approaching and is_delayed
        cta_standardized['status'] = cta_standardized.apply(
            lambda row: ';'.join(filter(None, [
                'Approaching' if row.get('is_approaching') == '1' else None,
                'Delayed' if row.get('is_delayed') == '1' else None
            ])), axis=1
        )
    
    # Combine all standardized datasets
    integrated_data = pd.concat([mbta_standardized, wmata_standardized, cta_standardized], ignore_index=True)
    
    # Select and order columns for final dataset
    columns = [
        'transit_system', 'route_id', 'station_id', 'station_name', 'vehicle_id',
        'direction_id', 'arrival_time', 'departure_time', 'status', 'prediction_id',
        'latitude', 'longitude', 'heading', 'data_type', 'recorded_at'
    ]
    
    # Select only columns that exist in the dataframe
    existing_columns = [col for col in columns if col in integrated_data.columns]
    integrated_data = integrated_data[existing_columns]
    
    # ISSUE #3: Save integrated data as CSV for local testing
    integrated_data.to_csv("./data/csv/integrated_transit_data.csv", index=False)
    logger.info(f"Saved integrated data to CSV with {len(integrated_data)} rows")
    
    return integrated_data

def main():
    """Main function to run the entire pipeline - ISSUE #3: Local pipeline execution"""
    logger.info("Starting local transit data integration pipeline")
    
    # Fetch data from all sources
    mbta_data = fetch_mbta_data()
    wmata_data = fetch_wmata_data()
    cta_data = fetch_cta_data()
    
    # Create integrated dataset
    integrated_data = create_integrated_dataset(mbta_data, wmata_data, cta_data)
    
    logger.info("Local transit data integration pipeline completed successfully")
    
    # Print summary
    print("\n=== Pipeline Execution Summary (ISSUE #3) ===")
    print(f"MBTA Data: {len(mbta_data)} rows")
    print(f"WMATA Data: {len(wmata_data)} rows")
    print(f"CTA Data: {len(cta_data)} rows")
    print(f"Integrated Data: {len(integrated_data)} rows")
    print(f"Output CSV files saved to ./data/csv/")
    print("===============================\n")

if __name__ == "__main__":
    main()