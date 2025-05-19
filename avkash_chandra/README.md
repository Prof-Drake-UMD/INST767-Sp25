# Real-Time Music & Weather Data Pipeline on GCP

This project implements a real-time data pipeline to ingest music and weather data from multiple public APIs, process it using Google Cloud Functions, and load the transformed data into BigQuery for analysis. The pipeline uses Google Cloud Pub/Sub to enable decoupled and scalable message passing between ingestion and transformation components.



Folder created using firstname_lastname format  
Selected 3 dynamic APIs with regularly updated data:
- Last.fm API
- Genius API
- OpenWeatherMap API  
Built a workflow that:
- Pulls, enriches, and transforms API data
- Publishes data through Google Pub/Sub
- Loads data into BigQuery  
Created analytical queries on the integrated dataset  



## Architecture Overview

The data pipeline follows these main stages:

### 1. Data Ingestion (Cloud Functions — HTTP Triggered)
- **Last.fm**: Fetches top tracks by tag or artist
- **Genius**: Fetches lyrics and metadata
- **OpenWeatherMap**: Retrieves weather data for given locations

> Each function publishes raw JSON to a dedicated **Pub/Sub topic** for downstream processing.

### 2. Data Transformation (Cloud Functions — Pub/Sub Triggered)
- Subscriber functions consume messages from Pub/Sub
- Data is validated, enriched, and standardized
- Final processed data is loaded into **BigQuery tables**
- Includes retry logic for handling race conditions and transient errors

### 3. Storage and Analysis (BigQuery)
- Tables: `merged_songs`, `song_weather`, `song_moods`, etc.
- Tables are partitioned by `ingest_timestamp` for performance
- Analytical queries extract trends and correlations between music and weather

---

## 📁 Project Structure

avkash_chandra/
│
├── ingest_function/ # Cloud Function for ingestion
│ ├── Genius.py
│ ├── Lastfm.py
│ ├── Openweather.py
│ ├── ingest_pub.py
│ ├── main.py
│ ├── inspect_json.py
│ ├── test_genius.py
│ ├── weather_data.json
│ └── requirements.txt
│
├── transform_function_cloud/ # Cloud Function for transformation & BigQuery load
│ ├── main.py
│ └── requirements.txt
│
├── create_tables.sql # SQL schemas for BigQuery tables
├── create_tables.py # Python script to create tables programmatically
├── build_dataset.py # Populates BigQuery tables using local scripts
├── merged.py # Local testing of merged data
├── run_all.py # Master script to run ingestion and transformation
├── output.csv # Sample output
├── queries.sql # Analytical queries
├── example_queries.py # Sample query executor
├── export_csv.py # Exports BigQuery data to CSV
└── script.py # Utility script for orchestrated runs
