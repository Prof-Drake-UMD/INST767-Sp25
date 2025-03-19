
import requests
import os
import json
from dotenv import load_dotenv

load_dotenv() 

target_states_cities = {
    "DC": "Washington",
    "MD": "Baltimore",
    "VA": "Richmond",
    "WV": "Charleston",
    "PA": "Philadelphia",
    "NJ": "Newark",
    "DE": "Wilmington",
    "OH": "Columbus",
    "KY": "Louisville"
}

# Fetch carbon emissions data from Carbon Interface API for the target states.
def get_carbon_emissions(electricity_unit, electricity_value):
    results = {}
    for state in target_states_cities.keys():
        country = "US"
        url = "https://www.carboninterface.com/api/v1/estimates"
        headers = {"Authorization": f"Bearer {os.getenv('CARBON_API_KEY')}", "Content-Type": "application/json"}
        payload = {
            "type": "electricity",
            "electricity_unit": electricity_unit,
            "electricity_value": float(electricity_value),
            "country": country,
            "state": state
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 201:
            data = response.json()
            # Extract carbon data from response
            estimated_at = data.get('data', {}).get('attributes', {}).get('estimated_at')
            electricity_value = data.get('data', {}).get('attributes', {}).get('electricity_value')
            electricity_unit = data.get('data', {}).get('attributes', {}).get('electricity_unit')
            carbon_g = data.get('data', {}).get('attributes', {}).get('carbon_g')
            carbon_lb = data.get('data', {}).get('attributes', {}).get('carbon_lb')
            carbon_kg = data.get('data', {}).get('attributes', {}).get('carbon_kg')
            key = data.get('data', {}).get('attributes', {}).get('state')
                        
            state_data = key.upper()
            results[state_data] = {
                'estimated_at': estimated_at,
                'electricity_value':  electricity_value,
                'electricity_unit':  electricity_unit,
                'carbon_g': carbon_g,
                'carbon_kg': carbon_kg,
                'carbon_lb': carbon_lb
            }
        else:
            print(f"Error fetching data for {state}: {response.status_code}")
    
    return results

 # Fetch real-time electricity carbon intensity from ElectricityMap API for the US-MIDA-PJM zone (covers all target states).
def get_electricity_data():   
    zone = "US-MIDA-PJM"
    url = f"https://api.electricitymap.org/v3/power-breakdown/latest?zone={zone}"
    headers = {"auth-token": os.getenv('ELECTRICITYMAP_API_KEY')}
    response = requests.get(url, headers=headers)
    return response.json() if response.status_code == 200 else response.text

# Fetch weather data from WeatherBit API using major cities in each target state.   
def get_weather_data():   
    results = {}
    for state, city in target_states_cities.items():
        url = f"https://api.weatherbit.io/v2.0/current?city={city}&country=US&key={os.getenv('WEATHERBIT_API_KEY')}"
        response = requests.get(url)
        results[state] = response.json() if response.status_code == 200 else response.text
    
    return results

def save_json(data, filename):
   # Save JSON data to a file.
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    carbon_data = get_carbon_emissions("mwh", 42)
    electricity_data = get_electricity_data()
    weather_data = get_weather_data()
    
    save_json(carbon_data, "carbon_data.json")
    save_json(electricity_data, "electricity_data.json")
    save_json(weather_data, "weather_data.json")
