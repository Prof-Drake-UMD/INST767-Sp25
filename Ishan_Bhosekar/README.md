# Climate & Economic Data Pipeline (GCP | Airflow | BigQuery | Pub/Sub)

![image](https://github.com/user-attachments/assets/c7725acd-9106-4904-a486-132b1a2be945)


## Project Overview
This project builds a robust, cloud-native data pipeline that ingests, transforms, and analyzes environmental and economic data using public APIs. It focuses on integrating weather, air quality, and GDP metrics for major global cities to uncover insights related to climate patterns and their economic implications.
 
## 🧱 Architecture Components
1. API Ingestion
Written modularly in Python with clear fetch_* functions.

Each fetch function handles JSON normalization and returns clean dictionaries.

2. Data Pipeline Using Airflow (Cloud Composer)
Three DAGs:

climate_data_pipeline: Extracts data, transforms, and loads to BigQuery.

publish_to_pubsub_dag: Publishes messages to Cloud Pub/Sub.

subscribe_from_pubsub_pipeline: Subscribes to messages and processes them asynchronously.

Asynchronous architecture using Cloud Pub/Sub as a message broker to decouple ingest and transformation stages.


> 📌 **Note:** All functional components like `ingest/`, `transform/`, `load/`, and `PubSub/` are contained within the `dag/` folder for modularity and Airflow DAG support.


## 📡 Data Sources
| API Source | Description |
| --- | --- |
| OpenWeatherMap API | Real-time weather data |
| WAQI API | Global air quality index data |
| World Bank API | Historical GDP (NY.GDP.MKTP.CD) per country |


## 🛠️ Technologies Used
Google Cloud Platform (GCP): BigQuery, Cloud Composer, Cloud Pub/Sub, Cloud Functions

Python 3.11: Data ingestion, transformation, and DAG logic

Apache Airflow: Scheduling and orchestration

Pub/Sub: Event-driven decoupling of ingestion and transformation

BigQuery Views: Analytical queries and data modeling


## 🗃️ BigQuery Schema Design
The data model is designed for analytics-first querying in BigQuery, leveraging its strengths in denormalized, columnar storage and scalable joins. While three primary tables—weather, air_quality, and gdp—store ingested and transformed data, two auxiliary structures enable effective analysis: a mapping table for relational alignment and a combined view for unified querying.

🔑 Main Tables (Denormalized Structure)
weather: Stores temperature, humidity, wind speed, weather descriptions, and derived flags like temperature_category and high_humidity_flag.

air_quality: Includes AQI and pollutant data (PM2.5, PM10, NO₂, etc.), with aqi_category as a derived field.

gdp: Holds multi-year GDP values per country, along with computed gdp_growth_rate.

🌍 city_country_mapping Table
This helper table provides the relational bridge between city names in weather and air_quality tables and country codes in the gdp table. Since the APIs return city names without standardized ISO country codes, this lookup table was manually curated to ensure accurate joins across datasets.

This table is not a core analytical table but is essential for ensuring consistent and correct country-level joins across different datasets, especially when aggregating or comparing against GDP.

🧩 combined_view: Unified Analytical Layer
The combined_view is a materialized SQL view that merges the most recent available data from all three domains—weather, air quality, and GDP—per city-country.

Design Highlights:
Joins weather and air quality data by city name.

Joins GDP by country code, linked through the city_country_mapping table.

Uses ROW_NUMBER() window function to ensure the latest snapshot per city and most recent GDP value per country.

Supports deep analytical queries with a wide, rich schema.

The combined_view is the primary layer for exploratory data analysis and SQL insights, and abstracts away the need for complex joins in downstream queries or visualization tools like Tableau.


> 📌 **Note:** All functional components like `ingest/`, `transform/`, `load/`, and `PubSub/` are contained within the `dag/` folder for modularity and Airflow DAG support.



## 📉 Limitations & Considerations
API freshness: Some air quality or weather APIs return stale or missing data (e.g., null AQI).

Partial joins: Not all cities have matching GDP rows or recent AQI values, leading to partial data in the combined_view.

Temporal mismatch: Weather and air quality are near real-time; GDP is annual, so alignment is approximate.

Free Tier: Entire project is designed to stay within GCP's free-tier usage limits.


## 🔐 IAM Roles & Access Configuration
To ensure secure and functional execution of the pipeline across various GCP services, several Identity and Access Management (IAM) roles were assigned to the service accounts used by Cloud Composer (Airflow), Pub/Sub, and BigQuery components.

| **IAM Role**              | **Description**                                                                            | **Purpose**                                                                                     |
|---------------------------|--------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `BigQuery Admin`          | Administer all BigQuery resources and data                                                 | Required to create datasets, tables, and manage views directly from Airflow or scripts         |
| `BigQuery Data Viewer`    | View access to datasets and their contents                                                 | Allows viewing data tables, schemas, and query results during debugging                        |
| `BigQuery Job User`       | Run BigQuery jobs                                                                          | Enables Python scripts and Airflow DAGs to submit queries and load jobs                        |
| `Composer Worker`         | Worker-level access to run Airflow DAG tasks                                               | Needed for Cloud Composer environment to execute DAG tasks                                     |
| `Logs Writer`             | Permission to write logs to Cloud Logging                                                  | Enables logging from Airflow tasks, publisher, and subscriber functions                        |
| `Service Account User`    | Allows a user to act as a service account                                                  | Needed to let Composer use the correct service identity to perform actions                     |
| `Storage Object Admin`    | Full access to create, view, and delete objects in GCS                                     | Required to upload, manage, and sync DAG files and scripts to the Composer bucket              |


## 📊 Use Case
This project answers analytical questions such as:

How does air quality correlate with weather conditions across cities?

What are the GDP growth trends in relation to climate patterns?

How do temperature and GDP growth interact across major economies?



