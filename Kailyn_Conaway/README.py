
## Flood and Housing Price Data

### Goal: My goal is to find areas in Baltimore that have above average capacity for floods and other severe weather and if it affects the price of homes. I'll be using three APIs, severe weather limited to Maryland, floods limited to Baltimore, and housing limited to Maryland. 

import requests
import json

#### 1. Weather Severity
#defining the api with chosen parameters
def md_weather_severe(area="MD", severity="Severe", limit=3):
    url = "https://api.weather.gov/alerts"
    params = {
    'area': "MD",
    'limit': 3,
    'severity': "Severe"
    }

response1 = requests.get(url, params=params, )

#retrieving json

if response1.status_code == 200:
    data1 = response1.json()
    print(data1)
else:
    print(f"Error: {response1.status_code}")

#### 2. Flood Data
#defining the api with chosen parameters

def baltimore_flood(lat=39.2905, lon=76.6104, limit=3):
    url = "http://nationalflooddata.com/"

    headers = {
        "x-api-key": "rfLkzNIzvl8RnUdxVYfbDaeTpyanba1h6KLvghZp"
    }
    params = {
    'lat': 39.2905,
    'lon': 76.6104,
    'limit': 3
    }

response2 = requests.get(url, params=params headers=headers)

#retrieving json

if response2.status_code == 200:
    data2 = response2.json()
    print(data2)
else:
    print(f"Error: {response2.status_code}")

#### 3. Housing Data
#defining the api with chosen parameters

def md_housing(state="Maryland", limit=3)
    url = "https://api.simplyrets.com/"
    params = {
        'state': 'Maryland',
        'limit': 3
    }

response3 = requests.get(url, params=params)

#retrieving json

if response3.status_code == 200:
    data3 = response3.json()
    print(data3)
else:
    print(f"Error: {response3.status_code}")

#### JSONs will return a python object including the attributed from the data considering the given parameters.