# Climate & Economic Data Pipeline (GCP | Airflow | BigQuery | Pub/Sub)

![image](https://github.com/user-attachments/assets/c7725acd-9106-4904-a486-132b1a2be945)


## Project Overview
This project builds a robust, cloud-native data pipeline that ingests, transforms, and analyzes environmental and economic data using public APIs. It focuses on integrating weather, air quality, and GDP metrics for major global cities to uncover insights related to climate patterns and their economic implications.
 
Built on the Google Cloud Platform (GCP), the system uses:

Cloud Composer (Airflow) for orchestration

Cloud Pub/Sub for asynchronous message passing

BigQuery for data storage, transformation, and analysis

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

## 📊 Use Case
This project answers analytical questions such as:

How does air quality correlate with weather conditions across cities?

What are the GDP growth trends in relation to climate patterns?

Which cities show climate-health risks based on AQI and humidity?

How do temperature and GDP growth interact across major economies?



