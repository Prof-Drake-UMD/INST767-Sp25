# Real-time Book Data Pipeline on GCP

This project implements a data pipeline to ingest book data from multiple public APIs, process it using serverless cloud functions, and load it into BigQuery for analysis. The pipeline is orchestrated using Apache Airflow running on Google Cloud Composer.

## Project Status

This project is part of a course assignment that requires building a data pipeline on Google Cloud Platform. The implementation satisfies the requirements specified in the course:

1. ✅ Created a folder with firstname_lastname format
2. ✅ Selected 3 different data APIs with consistently updating data:
   - New York Times Bestsellers API
   - Open Library API
   - Google Books API
3. ✅ Created a workflow that:
   - Pulls data from APIs
   - Processes and transforms the data
   - Loads data into BigQuery
4. ✅ Implemented a message sharing pattern using Google Cloud Pub/Sub
5. ✅ Created analytical queries for the integrated data


## Architecture Overview

The pipeline follows these main steps:
1. **Data Ingestion (Cloud Functions - HTTP Triggered, Invoked by Airflow):**
   * New York Times Bestsellers API
   * Open Library API
   * Google Books API
   These functions fetch data and publish the raw responses to dedicated Pub/Sub topics.

2. **Data Processing (Cloud Functions - Pub/Sub Triggered):**
   * Separate subscriber functions for each data source
   * Each subscriber implements a transformer and loader pattern
   * Transformers standardize the data format
   * Loaders handle BigQuery operations with retry logic for concurrent updates
   * Data is merged (upserted) into respective BigQuery tables using staging tables

3. **Orchestration (Cloud Composer - Airflow):**
   * An Airflow DAG defines the workflow, dependencies, and scheduling
   * It invokes the ingest Cloud Functions with appropriate parameters
   * Manages the overall flow with monitoring and retry capabilities

4. **Data Storage & Analysis (BigQuery):**
   * Partitioned tables store data from each source
   * Tables are partitioned by ingest_date for efficient querying
   * Analytical queries can be run across these tables

## Project Structure

```
.real-time-book-data-gcp/
├── .env.example                # Example environment variables template
├── .gitignore
├── book_pipeline_consumer_func/ # Python package for Cloud Functions & shared logic
│   ├── __init__.py
│   ├── api_ingest/             # Modules for ingesting data from APIs
│   │   ├── __init__.py
│   │   ├── base_ingest_handlers.py
│   │   ├── google_books.py
│   │   ├── nyt_books.py
│   │   └── open_library.py
│   ├── subscribers/            # Cloud Function subscribers for Pub/Sub topics
│   │   ├── __init__.py
│   │   ├── base_handler.py     # Base classes for message handling and data transformation
│   │   ├── process_google_books_subscriber.py
│   │   ├── process_nyt_subscriber.py
│   │   └── process_open_library_subscriber.py
│   └── requirements.txt        # Python dependencies for Cloud Functions
├── dags/
│   └── book_data_ingestion_dag.py # Airflow DAG for orchestration
├── sql/
│   ├── analytical_queries.sql  # Example analytical queries for BigQuery
│   ├── google_books_schema.sql # BigQuery schema for Google Books data
│   ├── nyt_books_schema.sql    # BigQuery schema for NYT Bestsellers data
│   └── open_library_books_schema.sql # BigQuery schema for Open Library data
├── setup_gcp_resources.sh      # Shell script to create/update GCP infrastructure
└── deploy_to_composer.sh       # Shell script to deploy DAGs and custom code to Composer
```

## Key Features

1. **Robust Data Processing:**
   - Each data source has its own transformer and loader
   - Standardized error handling and logging
   - Data validation and cleaning

2. **Concurrent Update Handling:**
   - Retry logic with exponential backoff for BigQuery operations
   - Staging tables for safe MERGE operations
   - Proper cleanup of temporary resources

3. **Efficient Data Storage:**
   - Tables partitioned by ingest_date
   - Optimized schemas for each data source
   - Support for incremental updates

## Prerequisites

* Google Cloud SDK (`gcloud`) installed and configured
* Python 3.9+ installed
* `pip` for Python package management
* An active Google Cloud Platform project with billing enabled
* API Keys for:
  * New York Times API
  * Google Books API

## Environment Setup

1. **Clone the repository:**
   ```bash
   git clone <repository_url>
   cd real-time-book-data-gcp
   ```

2. **Create a Python virtual environment:**
   ```bash
   python3 -m venv env
   source env/bin/activate
   ```

3. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up Environment Variables:**
   * Copy the `.env.example` file to `.env`:
     ```bash
     cp .env.example .env
     ```
   * Edit the `.env` file with your specific values for API keys, GCP project ID, etc.

## GCP Infrastructure Setup

Run the `setup_gcp_resources.sh` script to create and configure the necessary GCP resources:

```bash
source .env
chmod +x setup_gcp_resources.sh
./setup_gcp_resources.sh
```

## Deployment to Cloud Composer

1. **Ensure your Cloud Composer environment is running**

2. **Run the `deploy_to_composer.sh` script:**
   ```bash
   source .env
   chmod +x deploy_to_composer.sh
   ./deploy_to_composer.sh
   ```

## Cloud Functions Overview

### Ingest Functions (HTTP Triggered)
* `nyt_http_and_pubsub_trigger`: Fetches NYT bestsellers
* `open_library_http_and_pubsub_trigger`: Fetches Open Library data
* `google_books_http_and_pubsub_trigger`: Fetches Google Books data

### Subscriber Functions (Pub/Sub Triggered)
* `process_nyt_data_subscriber`: Processes NYT data
* `process_open_library_data_subscriber`: Processes Open Library data
* `process_google_books_data_subscriber`: Processes Google Books data

Each subscriber implements:
- Message decoding and validation
- Data transformation
- BigQuery loading with retry logic
- Proper error handling and logging

## BigQuery Schemas

Schema definitions for the BigQuery tables are located in the `sql/` directory:
* `sql/nyt_books_schema.sql`
* `sql/open_library_books_schema.sql`
* `sql/google_books_schema.sql`

Example analytical queries can be found in `sql/analytical_queries.sql`.

## Future Enhancements

* Implement Dead-Letter Queues (DLQs) for Pub/Sub subscriptions
* Add more comprehensive data quality checks
* Set up monitoring and alerting
* Implement CI/CD pipeline for automated deployment