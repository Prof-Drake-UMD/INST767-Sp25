# Open FDA Drugs Adverse Events Data Pipeline on GCP

### 🌐 APIs Overview

This project integrates multiple API data sources to retrieve and analyze drug-related information from OpenFDA apis. The APIs used in this project include:

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

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Technologies Used

In this project we are using google following cloud services:
1. Cloud Composer (Apache Airflow for running dags)
2. Google PubSub (Message Queue For asynchronous ETL pipeline)
3. Cloud Functions (Triggered by PubSub for transformation and loading into Big Query)
4. Google Big Query (For Data Warehousing and Analytical Queries)

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## Project Structure

<details>
<summary><strong>📁 Project Structure</strong></summary>

<br>

```plaintext
├── cloudFunctionTransform/
│   ├── load_to_bq.py
│   ├── main.py
│   ├── read_from_uri.py
│   ├── requirements.txt
│   └── transform.py
│
├── dags/
│   ├── extract_ndc_directory.py
│   ├── extract_recall_enforcement.py
│   └── open_fda_drug_events.py
│
├── include/
│   └── extract_from_apis/
│       ├── ExtractNdcChunk.py
│       └── ExtractRecallEnforcements.py
│
|   └── helpers/
│     ├── LoadBigQuery.py
│     ├── parse_gcs_uri.py
│     ├── PubSubHandler.py
│     └── StorageClients.py
│
|   └── openfdaAdverseEvents/
│     ├── code_maps.yml
│     ├── extract_raw_events_chunk.py
│     └── transform_events.py
│
|    └── sql/
|     ├── analytical_queries.sql
|     ├── combined_view.sql
|     ├── events_table.py
|     ├── ndc_createTable.sql
|     └── recall_enforcement_createTable.sql
|
└── deploy.sh
└── docker-compose.yml
└── Dockerfile
└── requirements.txt

 ```

</details>

  
## Workflow
### 1. Drug Adverse Events Pipeline
![SS1. Drug Adverse Events DAG](screenshots/DAGS/1.%20DAGS_Adverse_Events_DAG.png)

### Tasks

1. Check for api availability and number of records for a particular date
2. Calculate number chunks for pagination
3. Prepare chunk parameters
4. For each chunk fetch the data from api and store raw json in GCP Bucket
5. Run the transform function and load into GCP transformed folder
6. Load the data for particular date into date-partitioned big query table

### 2. NDC Directory and Recall Enforcements Pipeleine
![SS2. NDC Directory ETL DAG](screenshots/DAGS/2.%20DAGS_NDC_Directory.png)
![SS3. Recall Enforcement ETL DAG](screenshots/DAGS/3.%20DAGS_Recall_Enforcements_ETL.png)

### Tasks
1. Check for api availability and number of records for a particular date
2. Calculate number chunks for pagination
3. Prepare chunk parameters
4. For each chunk fetch the data from api,  store raw json in GCP Bucket, send a pub-sub message containing info about location of raw json file.
5. Pubsub triggers Cloud Function which transfroms the data and load it into staging table.
6. A merge query is implemented to insert non matching reords in Big Query table.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

### GCP Buckets

![SS4. Drugs Adverse Events](screenshots/GCS_Buckets/4.%20Buckets_Drug_Eventst.png)

Check other screenshots at [`screenshots/GCS_Buckets`](https://github.com/Prof-Drake-UMD/INST767-Sp25/tree/main/Priyanshu_Gupta/screenshots/GCS_Buckets)

<br>

### Pub-Sub Topic Cloud Function Subscription
![SS7. Pub-sub Topic Subscription](screenshots/Cloud_Function_and_Pubsub/7.%20PubSub_Topic_Subscription.png)

<br>

### Cloud Function Definition
![SS8. Cloud Function](screenshots/Cloud_Function_and_Pubsub/8.%20Cloud_Run_Func_Trigg_By_Pubsub.png)
Check out logs of completed triggered function LOGS [`screenshots/Cloud_Funtion_and_Pubsub`](https://github.com/Prof-Drake-UMD/INST767-Sp25/tree/main/Priyanshu_Gupta/screenshots/Cloud_Function_and_Pubsub)

<br>

### Big Query Tables

Drug Adverse Events Table in BQ
![SS10. Drug Adverse Events](screenshots/BigQuery/10.%20BQ_Drug_Adverse_Events.png)

Apart from 3 tables from different apis, a combined view is created in BigQuery to join all the three tables based on <b>product_ndc </b>(unique identifier for drugs). Sql query for join defined [here](https://github.com/Prof-Drake-UMD/INST767-Sp25/tree/main/Priyanshu_Gupta/include/sql/combined_view.sql) 

Check out other big query screen shots at [`screenshots/BigQuery`](https://github.com/Prof-Drake-UMD/INST767-Sp25/tree/main/Priyanshu_Gupta/screenshots/BigQuery)

You can find all the SQL `CREATE TABLE` statements in the [`include/sql/`](https://github.com/Prof-Drake-UMD/INST767-Sp25/tree/main/Priyanshu_Gupta/include/sql) folder.

<br>

### Analytical Questions

You can find all the SQL queries [`here`](https://github.com/Prof-Drake-UMD/INST767-Sp25/tree/main/Priyanshu_Gupta/include/sql/analytical_queries.sql) file.

Check out results of the queries at [`screenshots/Queries`](https://github.com/Prof-Drake-UMD/INST767-Sp25/tree/main/Priyanshu_Gupta/screenshots/Queries)


## Challenges and Solution Approcach
1. <b>Large Amount of Drug Adeverse Events</b>: The api has around 19 million events with deeply nested structure which makes it difficult to extract all the events. So, I decided to extract from 2024 to the present only. Created a dag that extracts only the events from a particular date and stores in partitioned bucket and big query table
   
2. <b>Cloud Function Deployment</b>: Faced difficulty in deploying cloud function that can be asynchronously triggered by pubsub whenever extract dag is run. Had to creat a seperate folder for cloud function with all helper utilities to deploy usin shell scripting.
   
3. <b>Idempotent Pipeline for large Table </b>: To prevent duplicate records in BigQuery when the Events DAG is run multiple times for the same date, I implemented a partitioned BigQuery table using receivedate as the partition key. By applying the WRITE_TRUNCATE write disposition to the specific partition, the pipeline now safely replaces only the relevant date’s data during each run, ensuring idempotency and preventing duplication.
   
4. <b>Idempotent Pipeline for smaller table </b>: For smaller datasets such as the NDC directory and recall events, I adopted a staging-based approach. New data is first loaded into a staging table, and then a MERGE statement is used to insert only the new records into the target table.
   
5. <b> Data Model Decisison </b>: The Drug Adverse Event Table had multiple reations and drugs associated, so I decided to keep these fields as repeated records in Big Query which is like a Struct and unnest it later for create a combined view.








