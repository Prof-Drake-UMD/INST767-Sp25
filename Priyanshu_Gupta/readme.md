# Open FDA Drugs Adverse Events Data Pipeline on GCP

## APIs Overview

This project integrates multiple API data sources to retrieve and analyze drug-related information from OpenFDA apis. The APIs used in this project include:

OpenFDA Drug Events API

NDC Directory API

Recall Enforcement API

### API Data Sources and Expected Data

### 1. OpenFDA Drug Events API

#### -- Endpoint: 
https://api.fda.gov/drug/event.json

#### -- Inputs:
search (e.g., search=receivedate:20240101&patient.drug.medicinalproduct:"aspirin")

limit (number of records to return)

skip (number of records to skip for pagination workflow as there is limit of 1000 records per request)

#### -- Expected Output:
Adverse event reports associated with specific drugs, including patient reactions, drug names, and outcomes.


### 2. NDC Directory API

#### - Endpoint: 
https://api.fda.gov/drug/ndc.json

#### -- Inputs:
search (e.g., search=brand_name:"Advil")

limit (number of records to return)

#### -- Expected Output:
National Drug Code (NDC) information, including active ingredients, manufacturer details, and marketing status.

### 3. Recall Enforcement API
#### Description: 
Recalls are an appropriate alternative method for removing or correcting marketed consumer products, their labeling, and/or promotional literature that violate the laws administered by the Food and Drug Administration (FDA)
#### -- Endpoint: 
https://api.fda.gov/drug/enforcement.json

#### -- Inputs:
search (e.g., search=openfda.brand_name:"Tylenol")

limit (number of records to return)

#### -- Expected Output:
Enforcement reports about drug product recalls including recall classification, reason for recall, recall status, and recall intitiation date, termination date.

## Technologies Used

In this project we are using google following cloud services:
1. Cloud Composer (Apache Airflow for running dags)
2. Google PubSub (Message Queue For asynchronous ETL pipeline)
3. Cloud Functions (Triggered by PubSub for transformation and loading into Big Query)
4. Google Big Query (For Data Warehousing and Analytical Queries)

## Workflow
### 1. Drug Adverse Events Pipeline
![SS1. Drug Adverse Events DAG](screenshots/1.%20DAGS_Adverse_Events_DAG.png)

1. Check for api availability and number of records for a particular date
2. Calculate number chunks for pagination
3. Prepare chunk parameters
4. For each chunk fetch the data from api and store raw json in GCP Bucket
5. Run the transform function and load into GCP transformed folder
6. Load the data for particular date into date-partitioned big query table

### 2. NDC Directory and Recall Enforcement Events Table
![SS2. NDC Directory ETL DAG](screenshots/2.%20DAGS_NDC_Directory.png)
![SS3. Recall Enforcement ETL DAG](screenshots/3.%20DAGS_Recall_Enforcements_ETL.png)

1. Check for api availability and number of records for a particular date
2. Calculate number chunks for pagination
3. Prepare chunk parameters
4. For each chunk fetch the data from api,  store raw json in GCP Bucket, send a pub-sub message containing info about location of raw json file.
5. Pubsub triggers Cloud Function which transfroms the data and load it into staging table.
6. A merge query is implemented to insert non matching reords in Big Query table.

