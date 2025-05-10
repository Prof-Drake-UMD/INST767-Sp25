# api_helpers.py
import requests
import json
import logging
from config import MBTA_API, WMATA_API, CTA_API

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_mbta_data(endpoint, params=None):
    """
    Fetch data from MBTA API
    
    Args:
        endpoint (str): API endpoint to call
        params (dict, optional): Additional parameters for the request
        
    Returns:
        dict: JSON response data
    """
    url = f"{MBTA_API['base_url']}{endpoint}"
    headers = {"x-api-key": MBTA_API['api_key']} if MBTA_API['api_key'] else {}
    
    try:
        logger.info(f"Fetching data from MBTA API: {url}")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching MBTA data: {e}")
        raise

def fetch_wmata_data(endpoint, params=None):
    """
    Fetch data from WMATA API
    
    Args:
        endpoint (str): API endpoint to call
        params (dict, optional): Additional parameters for the request
        
    Returns:
        dict: JSON response data
    """
    url = f"{WMATA_API['base_url']}{endpoint}"
    headers = {"api_key": WMATA_API['api_key']}
    
    try:
        logger.info(f"Fetching data from WMATA API: {url}")
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching WMATA data: {e}")
        raise

def fetch_cta_data(endpoint, params=None):
    """
    Fetch data from CTA API
    
    Args:
        endpoint (str): API endpoint to call
        params (dict, optional): Additional parameters for the request
        
    Returns:
        dict: JSON or XML response data (converted to dict if XML)
    """
    url = f"{CTA_API['base_url']}{endpoint}"
    
    # Add API key to parameters
    if params is None:
        params = {}
    
    if CTA_API['api_key'] != "YOUR_CTA_API_KEY":
        params['key'] = CTA_API['api_key']
    
    # Add outputType=JSON to get JSON response if applicable
    params['outputType'] = 'JSON'
    
    try:
        logger.info(f"Fetching data from CTA API: {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        # Try to parse as JSON first
        try:
            return response.json()
        except json.JSONDecodeError:
            logger.warning("Response is not JSON, returning text content")
            return response.text
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching CTA data: {e}")
        raise

# Specific functions for fetching different types of transit data

def get_mbta_routes():
    """Get all MBTA routes"""
    return fetch_mbta_data(MBTA_API['endpoints']['routes'])

def get_mbta_predictions(route_ids=None):
    """
    Get current MBTA predictions for specific routes.

    Args:
        route_ids (list or str, optional): A list or comma-separated string of route IDs.
                                          Example: ['Red', 'Green-B'] or "Red,Green-B". Defaults to None.

    Returns:
        dict: JSON response data for predictions.

    Raises:
        ValueError: If route_ids is not provided, as the endpoint requires filters.
    """
    params = {}
    if route_ids:
        route_filter_value = ','.join(route_ids) if isinstance(route_ids, list) else route_ids
        params['filter[route]'] = route_filter_value
        logger.info(f"Requesting MBTA predictions for routes: {route_filter_value}")
    else:
        logger.error("MBTA /predictions endpoint requires filters (e.g., filter[route]).")
        raise ValueError("MBTA /predictions requires route filters.")

    endpoint = MBTA_API['endpoints']['predictions']
    return fetch_mbta_data(endpoint, params=params)

def get_mbta_vehicles():
    """Get current MBTA vehicle positions"""
    return fetch_mbta_data(MBTA_API['endpoints']['vehicles'])

def get_wmata_rail_stations():
    """Get all WMATA rail stations"""
    return fetch_wmata_data(WMATA_API['endpoints']['rail_stations'])

def get_wmata_rail_predictions():
    """Get current WMATA rail predictions"""
    return fetch_wmata_data(WMATA_API['endpoints']['rail_predictions'])

def get_cta_train_arrivals(station_id=None):
    """
    Get CTA train arrival predictions
    
    Args:
        station_id (str, optional): The station ID to get predictions for
        
    Returns:
        dict: Train arrival predictions
    """
    params = {}
    if station_id:
        params['mapid'] = station_id
    
    return fetch_cta_data(CTA_API['endpoints']['train_arrivals'], params)

def get_cta_train_positions(route_ids): # Route(s) are required
    """
    Get CTA train positions for specified routes using the ttpositions.aspx endpoint.

    Args:
        route_ids (list or str): A list or comma-separated string of route IDs (e.g., ['Red','Blue'] or "Red,Blue").

    Returns:
        dict: JSON response data for train positions.

    Raises:
        ValueError: If route_ids is not provided.
    """
    if not route_ids:
        logger.error("CTA /positions endpoint requires route filters (rt).")
        raise ValueError("CTA /positions endpoint requires route filters (rt).")

    # API expects 'rt' param with comma-separated values
    route_param_value = ','.join(route_ids) if isinstance(route_ids, list) else route_ids
    params = {'rt': route_param_value}

    logger.info(f"Requesting CTA positions for routes: {route_param_value}")
    endpoint = CTA_API['endpoints']['train_positions']
    # Assuming fetch_cta_data handles the request and JSON parsing/error checking
    return fetch_cta_data(endpoint, params=params)