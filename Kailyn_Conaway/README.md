# Kailyn Conaway - Big Data Infrastructure Final Project (API Pulls)
## My project aims to look at the relationship between violent crime in Maryland and popular contributors to the increase or decrease in crime rates. I'll be using different counties/jurisdictions in Maryland to query the extent certain factors have on violent crime rate.
#### The variables who's relationship with violent crime will be as follows.
#### -  Social Resources
#### -  Teen Pregnancy
#### -  Education



#### 1. The first API I'll be pulling is for data on Maryland demographics by jurisdiction/county. This will be necessary to caluclate rates of particular variables.

#### Pulling data from API:
import requests \
import json
    
url = "https://opendata.maryland.gov/resource/pa7d-u6hs.json" \
response1 = requests.get(url)

#### Retrieving JSON from url if status is 200 (meaning data request was sucessful):
if response1.status_code == 200: \
    data1 = response1.json() \
    print(data1) \
else: \
    print(f"Error: {response1.status_code}")

#### 2. The next API I'll pull on violent crimes in the state of Maryland. I'm limiting the API to only pull from the year 2020 since this dataset is especially large.

#### Pulling data from the API:
import requests \
import json
    
url = "https://opendata.maryland.gov/resource/2p5g-xrcb.json?year=2020" \
response1 = requests.get(url)

#### Retrieving JSON from url if status is 200 (meaning data request was sucessful):
if response1.status_code == 200: \
    data1 = response1.json() \
    print(data1) \
else: \
    print(f"Error: {response1.status_code}")

#### 3. The last API I'll be pulling today (I will have more for the actual project) covers teen birth rates. I will also be limiting this returned JSON to the year 2020.

#### Pulling data from API:
import requests \
import json
    
url = "https://opendata.maryland.gov/resource/t8wg-hb7j.json?year=2020" \
response1 = requests.get(url)

#### Retrieving JSON from url if status is 200 (meaning data request was sucessful):
if response1.status_code == 200: \
    data1 = response1.json() \
    print(data1) \
else: \
    print(f"Error: {response1.status_code}")
