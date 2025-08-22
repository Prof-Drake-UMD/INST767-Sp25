# Kailyn Conaway - Big Data Infrastructure Final Project (Issue 1 - API Pulls)
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

## SQL Queries (Issue 2)
#### Join Demographics and Highschool Graduation totals to Calculate Rate of Graduates per County in 2018
<img width="840" height="696" alt="Image" src="https://github.com/user-attachments/assets/d71998b5-cf07-4b5f-bc16-b336c4e7cb84" />

#### Total Crime Divided by Population per County to Calculate Crime Rate in 2018
<img width="664" height="688" alt="Image" src="https://github.com/user-attachments/assets/5084b290-5b2f-4502-8325-00821c589542" />

#### Teen Pregnancy Rate per Ethnicity (Not important for overall project, just wanted to make a more interesting query)
<img width="664" height="638" alt="Image" src="https://github.com/user-attachments/assets/de09e3d1-b8f1-4e9d-a383-959298c12902" />

## DAG Creation (Issue 3)
### The goal of my DAG is to directly pull from the API, transform into a CSV, and add to my BigQuery Studio. There are several DAGs to pull from 4 different APIs but all follow the attached graph image below:
<img width="1015" height="308" alt="Image" src="https://github.com/user-attachments/assets/130e2679-ba30-4fd5-9623-e4c8d67fd9de" />

#### Code for DAGs attached below.
<img width="666" height="569" alt="Image" src="https://github.com/user-attachments/assets/5eef5cc3-d8dd-4591-ba3a-30826ff82ba3" />
