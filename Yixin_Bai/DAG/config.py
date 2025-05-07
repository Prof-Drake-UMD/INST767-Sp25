# config.py
import os

# API Configuration
MBTA_API = {
    "base_url": "https://api-v3.mbta.com",
    "api_key": "",  # MBTA API allows access without key (with rate limits)
    "endpoints": {
        "routes": "/routes",
        "predictions": "/predictions",
        "vehicles": "/vehicles"
    }
}

WMATA_API = {
    "base_url": "https://api.wmata.com",
    "api_key": "3c50f1b6216346579618a8f3cf121182",  # Replace with your actual API key
    "endpoints": {
        "rail_stations": "/Rail.svc/json/jStations",
        "rail_predictions": "/StationPrediction.svc/json/GetPrediction/All"
    }
}

CTA_API = {
    "base_url": "http://www.transitchicago.com/api",
    "api_key": "9c37edd717364621ab93a2ebc9d8d043",  # Replace with your actual API key
    "endpoints": {
        "train_arrivals": "/1.0/ttarrivals.aspx",
        "train_follow": "/1.0/ttfollow.aspx",
        "train_positions": "/1.0/ttpositions.aspx"
    }
}

# Google Cloud Configuration
GCP_CONFIG = {
    "project_id": "lithe-camp-458100-s5",  # Based on your screenshot
    "region": "us-east4",
    "zone": "us-east4-a"
}

# BigQuery Configuration
BQ_CONFIG = {
    "dataset_id": "transit_data",
    "tables": {
        "mbta": "mbta_data",
        "wmata": "wmata_data",
        "cta": "cta_data",
        "cta_positions": "cta_positions_data",
        "integrated": "integrated_transit_data"
    }
}

# Cloud Storage Configuration
GCS_CONFIG = {
    "bucket_name": "transit-data-bucket-767projt0-512xsics3-3x9289-03usbc",
    "temp_dir": "temp"
}

# Data Refresh Configuration
DATA_REFRESH = {
    "schedule_interval": "*/15 * * * *"  # Every 15 minutes
}