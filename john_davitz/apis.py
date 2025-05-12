import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import time


load_dotenv()

#NYC MTA API for DATA
url = "https://data.ny.gov/resource/wujg-7c2s.json"

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    #print(data)
    with open('john_davitz/metro.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)
else:
    print(f"Error: {response.status_code}")


#US Weather data for manhattan 2020-2024
url = "https://archive-api.open-meteo.com/v1/archive?latitude=40.7685&longitude=-73.9822&start_date=2025-05-01&end_date=2025-05-02&hourly=temperature_2m,rain,precipitation,weather_code&timezone=America%2FNew_York&temperature_unit=fahrenheit&wind_speed_unit=mph"

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    #print(data)
    with open('john_davitz/weather.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)
else:
    print(f"Error: {response.status_code}")




