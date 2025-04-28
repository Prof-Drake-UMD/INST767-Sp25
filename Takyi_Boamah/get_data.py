import os
import json
import time
import datetime
import logging
import requests
import concurrent.futures
# from dotenv import load_dotenv        # For  local access to environmental variable
# load_dotenv()

#Load API keys from .env file
EIA_API_KEY = os.getenv('EIA_API_KEY')
CARBON_API_KEY = os.getenv('CARBON_API_KEY2')
WEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_api_keys():
    if not all([EIA_API_KEY, CARBON_API_KEY, WEATHER_API_KEY]):
        logging.error("Missing one or more API keys. Check your .env file.")
        exit(1)
check_api_keys()

# -------------------------------
# 1. Fetch EIA Electricity Profile
# -------------------------------
def fetch_eia_data():
    url = "https://api.eia.gov/v2/electricity/state-electricity-profiles/emissions-by-state-by-fuel/data"
    params = {
        'api_key': EIA_API_KEY,
        'data[]': 'co2-thousand-metric-tons',
        "sort[0][column]": "period",
        "sort[0][direction]": "desc"
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json().get('response', {}).get('data', [])
    except requests.RequestException as e:
        logging.error(f"EIA API request failed: {e}")
        return []

# -------------------------------
# 3. Fetch Carbon Emissions from Carbon Interface
# -------------------------------
def get_carbon_emission_for_state(state, electricity_unit="mwh", electricity_value=42):
    """Fetches carbon emission data for a given state."""
    url = "https://www.carboninterface.com/api/v1/estimates"
    headers = {"Authorization": f"Bearer {CARBON_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "type": "electricity",
        "electricity_unit": electricity_unit,
        "electricity_value": electricity_value,
        "country": "US",
        "state": state
    }
    
    for _ in range(3):  # Retry mechanism
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json().get("data", {}).get("attributes", {})
            return {"state": state, **data}
        except requests.RequestException as e:
            logging.warning(f"Retrying {state} due to error: {e}")
            time.sleep(2)
    
    logging.error(f"Failed to fetch data for {state}")
    return None

def fetch_all_carbon_data():
    """Fetches carbon emissions for all U.S. states concurrently."""
    states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
              "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
              "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_state = {executor.submit(get_carbon_emission_for_state, state): state for state in states}
        for future in concurrent.futures.as_completed(future_to_state):
            result = future.result()
            if result:
                results.append(result)
    return results

# -------------------------------
# 4. Fetch Weather Data (OpenWeatherMap)
# -------------------------------
STATE_CAPITALS = {
    "AL": "Montgomery", "AK": "Juneau", "AZ": "Phoenix", "AR": "Little Rock", "CA": "Sacramento",
    "CO": "Denver", "CT": "Hartford", "DE": "Dover", "FL": "Tallahassee", "GA": "Atlanta", "HI": "Honolulu",
    "ID": "Boise", "IL": "Springfield", "IN": "Indianapolis", "IA": "Des Moines", "KS": "Topeka", "KY": "Frankfort",
    "LA": "Baton Rouge", "ME": "Augusta", "MD": "Annapolis", "MA": "Boston", "MI": "Lansing", "MN": "Saint Paul",
    "MS": "Jackson", "MO": "Jefferson City", "MT": "Helena", "NE": "Lincoln", "NV": "Carson City", "NH": "Concord",
    "NJ": "Trenton", "NM": "Santa Fe", "NY": "Albany", "NC": "Raleigh", "ND": "Bismarck", "OH": "Columbus",
    "OK": "Oklahoma City", "OR": "Salem", "PA": "Harrisburg", "RI": "Providence", "SC": "Columbia", "SD": "Pierre",
    "TN": "Nashville", "TX": "Austin", "UT": "Salt Lake City", "VT": "Montpelier", "VA": "Richmond", "WA": "Olympia",
    "WV": "Charleston", "WI": "Madison", "WY": "Cheyenne", "DC": "Washington"
}

def fetch_weather_data(city):
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': f"{city},US",
        'appid': WEATHER_API_KEY,
        'units': 'metric'
    }
    for _ in range(3):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.warning(f"Retrying weather API for {city} due to error: {e}")
            time.sleep(2)
    logging.error(f"Failed to fetch weather data for {city}")
    return None

def fetch_all_weather_data():
    weather_data = []
    for state, capital in STATE_CAPITALS.items():
        logging.info(f"Fetching weather for {capital}, {state}...")
        result = fetch_weather_data(capital)
        if result:
            weather_data.append({'state': state, 'city': capital, 'weather': result})
    return weather_data

# -------------------------------
# 5. Save JSON Data to File
# -------------------------------
def save_json(data, filename_prefix):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    os.makedirs("api_results", exist_ok=True)
    filepath = os.path.join("api_results", f"{filename_prefix}.json")
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    logging.info(f"Saved: {filepath}")

# -------------------------------
# 6. Main Execution
# -------------------------------
def main():
    logging.info("🌱 Starting EcoFusion data extraction pipeline...")

    eia_data = fetch_eia_data()
    save_json(eia_data, "state_electricity_profile")

    carbon_data = fetch_all_carbon_data()
    save_json(carbon_data, "carbon_estimates")

    weather_data = fetch_all_weather_data()
    save_json(weather_data, "weather_data")

    logging.info("✅ EcoFusion data extraction completed.")

if __name__ == "__main__":
    main()
