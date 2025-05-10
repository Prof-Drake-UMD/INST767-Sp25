# 🔍 Job Market Data Integration Pipeline  
A multi-source ETL project for job search using Google Cloud Platform



## 📘 Project Overview

This is the final project for INST767 (Sp25) in University of Maryland, focusing on building a **cloud data pipeline** using **Google Cloud Platform** tools. The goal is to **extract**, **transform**, and **load** data from multiple external APIs into **BigQuery** to enable further analysis.

As a person pursuing a Master's in HCI with a background in computer science, this project aims to provide people with similar background to have a simplified job search experience. This is a ETL pipeline that integrates data from three different job market APIs, which specifically tailored for data, engineer, and design roles. 


## 🧭 Objective

To write a DAG that intergrates multiple big data sources together, and create a system on Google Cloud. Three different data apis that have conistently updating data are selected.

## 🔗 Data Sources

| API | Focus | Update Frequency | Docs |
|-----|-------|------------------|------|
| **The Muse** | Creative & Design Jobs |  Daily at 12:00 UTC |[API Docs](https://www.themuse.com/developers/api/v2) |
| **Adzuna** | Technical / Engineering Roles | Every 6 hours | [API Docs](https://developer.adzuna.com/) |
| **Jooble** | Broad job listings (Entry level / Hourly Jobs) | Daily at 06:00 UTC | [API Docs](https://jooble.org/api/about) |



## 🧱 Components

###  Extraction (Ingest)
- Python modules (`adzuna_api.py`, `jooble_api.py`, `muse_api.py`)
- Retry logic and error handling
- Pulls raw data from APIs and writes to json files

### Transformation
- Converts inconsistent fields into a **standardized schema**
  
```json
{
  "source": "string",
  "job_title": "string",
  "job_description": "string",
  "job_url": "string",
  "posted_date": "string",
  "company_name": "string",
  "job_category": "string",
  "job_type": "string",
  "salary": "string",
}
```

### Loading
- Transformed files written to Google Cloud Storage
- Loaded into BigQuery table
- Run the dataset table with queries to analyze job market data



## 📁 File Structure
Since this project includes both local pipeline and upload to Google Cloud Platform, I separated files into different directories and kept all of them. 
- Files run on GCP are stored in `google_cloud` directory
- `pipeline.py` includes api fetch from files under `api_connection` and data transformation from `data_cleaning.py`
- `data` and `transformed_data` stored fetched data and transformed data separately
- `sql` contains dataset with table in `job_market_tables.sql` and queries in `job_market_queries.sql`

```
ChienChi_Liu/
├── README.md
├── DAGs/
│   ├── api_connection
│   │   ├── adzuna_api.py
│   │   ├── jooble_api.py
│   │   ├── muse_api.py
│   ├── data_cleaning
│   │   ├── jobs_cleaning.ipynb
│   │   ├── jobs_cleaning.py
│   ├── data
│   │   ├── adzuna_jobs.json
│   │   ├── jooble_jobs.json
│   │   ├── muse_jobs.json
│   ├── transformed_data
│   │   ├── jobs_data_standardized.csv
│   │   ├── jobs_data_standardized.json
│   ├── pipeline.py
│   ├── google_cloud
│   │   ├── ingest
│   │   │   ├── adzuna_api.py
│   │   │   ├── jooble_api.py
│   │   │   ├── muse_api.py
│   │   │   ├── main.py
│   │   │   ├── requirements.txt
│   │   ├── transform
│   │   │   ├── main.py
│   │   │   ├── requirements.txt
│   │   ├── screenshots
│   ├── sql
│   │   ├── job_market_queries.sql
│   │   ├── job_market_tables.sql
├── Dockerfile.fetch
└── Dockerfile.transform
```


## 📊 Analytical Use Cases

With the integrated dataset in BigQuery, I explore:

- Job Posting Trends: Analyze how job postings are changing by industry and company
- Top Hiring Companies: Identify leading employers in each job category
- New Companies Hiring: Discover recently active companies
- Key Job Platforms: Determine the most popular sources for job postings among three apis
- Salary Ranges: Understand typical pay scales across different job types and industries



