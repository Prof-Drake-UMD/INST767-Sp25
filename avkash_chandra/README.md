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

Ingestion Functions

Transformation Functions

BigQuery Setup

Integration & Orchestration Scripts

Data Output & Analysis
'''
avkash_chandra/
│
├── ingest_function/                    # Cloud Function (HTTP triggered) - Ingests API data
│   ├── main.py                         # Entry point for GCP deployment
│   ├── ingest_pub.py                   # Publishes enriched API data to Pub/Sub
│   ├── Genius.py                       # Genius API call and response handler
│   ├── Lastfm.py                       # Last.fm API call and response handler
│   ├── Openweather.py                  # OpenWeatherMap API call and response handler
│   ├── inspect_json.py                 # Tool for exploring raw API responses
│   ├── test_genius.py                  # Local test script for Genius ingestion
│   ├── weather_data.json               # Sample API response for testing
│   └── requirements.txt                # Dependencies for ingestion function
│
├── transform_function_cloud/           # Cloud Function (Pub/Sub triggered) - Transforms & loads data
│   ├── main.py                         # Entry point for subscriber/transformer
│   └── requirements.txt                # Dependencies for transformation function
│
├── create_tables.sql                   # BigQuery table definitions (DDL)
├── create_tables.py                    # Script to create BigQuery tables programmatically
├── build_dataset.py                    # Inserts example/test data into BigQuery
│
├── merged.py                           # Locally merges all data sources for testing
├── run_all.py                          # Orchestrates ingestion and transformation locally
├── export_csv.py                       # Exports BigQuery tables to CSV
├── output.csv                          # Sample output file
│
├── queries.sql                         # SQL queries for analysis
├── example_queries.py                  # Runs queries on BigQuery
└── script.py                           # Utility runner for quick tests
'''
