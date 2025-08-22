# Kailyn Conaway - Big Data Infrastructure Final Project
## My project aims to look at the relationship between violent crime in Maryland and popular contributors to the increase or decrease in crime rates. I'll be using different counties/jurisdictions in Maryland to query the extent certain factors have on violent crime rate.
#### The variables I'll be looking at to find relationship to violent crime will be as follows.
* Social Resources
* Teen Pregnancy
* Education
* Income

## Data Sources:
| API Source | EndPoint |
| --- | --- |
| MD Open Data - Demographics | resource/pa7d-u6hs.json |
| MD Open Data - Violent Crime| resource/2p5g-xrcb.json |
| MD Open Data - Teen Pregnancy| resource/t8wg-hb7j.json |
| MD Open Data - Education| resource/t8wg-hb7j.json |

## Services – Uses
#### Big Query – used to query data, connect DAGs
#### Python - used to write all code excluding SQL for querying (pull APIs,code DAGs, connect Cloud Run)
#### Pub/Sub – used to connect bucket to big query
#### Apache Airflow (used in console) - Create DAGs to connect API to Big Query
#### Cloud Run -  to run containers

## Schema
#### This dataset hopes to find trends in certain "risk factors" for violent crime to see the effect of the variable, if any. To do so, I'll be comparing between counties in the state. My main focus was to find rates or my available variables (rate of certain crimes, teen birth rate, average education, income, etc.) then comparing the results against overall violent crime.

### Table Structures (some were joined for certain queries)
<img width="399" height="181" alt="teen birth" src="https://github.com/user-attachments/assets/81ab0d70-c2c3-420b-83b6-6102c467fc64" />
<img width="460" height="261" alt="education" src="https://github.com/user-attachments/assets/9b5c9175-50cb-402a-a4ad-dfbf0ec5e882" />
<img width="486" height="325" alt="demographics" src="https://github.com/user-attachments/assets/5874b900-4e89-4e97-8b52-c50896794b06" />
<img width="482" height="626" alt="crime" src="https://github.com/user-attachments/assets/895193c9-ee80-41a4-8855-08877738c478" />

## Real-World Use Cases
#### Baltimore popularly saw a signifigant reduction in crime earlier this year - something many major cities around the world have concerns about currently. Knowing what risk factors have the highest correlation to violent crime would allow law makers to address these issues specifically. Analysis also allows us to ignore bias – for instance, despite being physically next to one another, Baltimore County has nearly half the rate of teenage pregnancies when compared to Baltimore City.

## Possible Issues
* These APIs do not update at the same time. While the demographics data is updated every few years, the violent crime data was updated on a yearly basis up until 2020.
* I only use commonly-accepted risk factors, there could be many more confounding variables that are not addressed.
* My Google Cloud has limited permissions due to using the free version.

## Below are screenshots and code used to complete the project.

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

## SQL Queries
#### Join Demographics and Highschool Graduation totals to Calculate Rate of Graduates per County in 2018
<img width="840" height="696" alt="Image" src="https://github.com/user-attachments/assets/d71998b5-cf07-4b5f-bc16-b336c4e7cb84" />

#### Total Crime Divided by Population per County to Calculate Crime Rate in 2018
<img width="664" height="688" alt="Image" src="https://github.com/user-attachments/assets/5084b290-5b2f-4502-8325-00821c589542" />

#### Teen Pregnancy Rate per Ethnicity (Not important for overall project, just wanted to make a more interesting query)
<img width="664" height="638" alt="Image" src="https://github.com/user-attachments/assets/de09e3d1-b8f1-4e9d-a383-959298c12902" />

## DAG Creation
### The goal of my DAG is to directly pull from the API, transform into a CSV, and add to my BigQuery Studio. There are several DAGs to pull from 4 different APIs but all follow the attached graph image below:
<img width="1015" height="308" alt="Image" src="https://github.com/user-attachments/assets/130e2679-ba30-4fd5-9623-e4c8d67fd9de" />

#### Code for DAGs attached below.
<img width="666" height="569" alt="Image" src="https://github.com/user-attachments/assets/5eef5cc3-d8dd-4591-ba3a-30826ff82ba3" />

## Cloud Run
<img width="1413" height="660" alt="Screenshot 2025-08-21 at 10 44 09 PM" src="https://github.com/user-attachments/assets/6776731b-6a31-4e41-a45c-c39e8cdb7884" />

<img width="840" height="443" alt="Screenshot 2025-08-21 at 10 43 02 PM" src="https://github.com/user-attachments/assets/4775ee74-6857-4b33-905e-2bb3714e3b06" />

## Pub/Sub
<img width="840" height="334" alt="Screenshot 2025-08-21 at 10 41 57 PM" src="https://github.com/user-attachments/assets/2991b81e-d11b-4180-8db5-872c9ec54a43" />
