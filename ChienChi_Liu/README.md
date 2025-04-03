# 🔍 Job Market Data Integration Pipeline  
*A multi-source ETL project for unified job search insights using Google Cloud Platform*



## 📘 Project Overview

<!-- This project is part of the final assignment for INST767 (Sp25), focusing on building a **cloud-native data pipeline** using **Google Cloud Platform** tools. The goal is to **extract**, **transform**, and **load** data from multiple external APIs into **BigQuery**, enabling further analysis and unified access via a single API. -->

As a person pursuing a Master's in HCI with a background in computer science, this project aims to provide people with similar background to have a simplified job search experience. This is a ETL pipeline that integrates data from three different job market APIs—tailored for tech, design, and freelance roles—and exposes them through a unified schema and API.



## 🧭 Objective

Build an automated data pipeline using **Apache Airflow** (via **Cloud Composer**) that:

- Pulls data from **three external job-related APIs**
- Transforms the data into a **unified schema**
- Loads the cleaned data into **BigQuery**


## 🔗 Data Sources

| API | Focus | Update Frequency | Docs |
|-----|-------|------------------|------|
| **The Muse** | Creative & Design Jobs | Daily | [API Docs](https://www.themuse.com/developers/api/v2) |
| **Adzuna** | Technical / Engineering Roles | Every 6 hours | [API Docs](https://developer.adzuna.com/) |
| **Hacker News: Who's Hiring** | Freelance / Startup Jobs | Monthly | [GitHub API](https://github.com/HackerNews/API) / [Search API](https://hn.algolia.com/api) |



## 🏗️ Architecture

The system follows a modular ETL pattern using Google Cloud services:

```
[ Muse / Adzuna / HN APIs ] 
        ↓
[ Python API Connectors ]
        ↓
[ Cloud Composer (Airflow DAG) ]
        ↓
[ GCS (intermediate storage) ]
        ↓
[ BigQuery (final storage & analysis) ]
```



## 🧱 Components

### 🛠️ Extraction
- Custom Python modules (`muse_connector.py`, etc.)
- Retry logic and error handling
- Pulls raw data from APIs and writes to Cloud Storage

### 🧼 Transformation
- Converts inconsistent fields into a **standardized schema**
- Cleans nulls, infers job types, standardizes skills and salary

### 🧩 Unified Schema

```json
{
  "job_id": "string",
  "title": "string",
  "company": "string",
  "location": "string",
  "description": "string",
  "salary_info": "string | null",
  "employment_type": "string",
  "posted_date": "date",
  "skills_required": ["string"],
  "experience_level": "string | null",
  "source_api": "string",
  "additional_metadata": "object | null"
}
```

### 📥 Loading
- Transformed files written to GCS in newline-delimited JSON
- Loaded into partitioned BigQuery table by `posted_date`

### 📅 Update Schedule

| Source | Schedule |
|--------|----------|
| The Muse | Daily at 12:00 UTC |
| Adzuna | Every 6 hours |
| Hacker News | Monthly, 1st day at 12:00 UTC |



<!-- ## 📁 File Structure

```
firstname_lastname/
├── README.md
├── dags/
│   ├── job_data_pipeline.py
│   └── modules/
│       ├── muse_connector.py
│       ├── adzuna_connector.py
│       ├── hackernews_connector.py
│       └── data_transformer.py
├── schemas/
│   └── unified_job_schema.json
└── sql/
    └── analysis_queries.sql
``` -->



## 🌐 API Layer 

The cleaned job data in BigQuery is exposed via a basic RESTful API.

### Base URL

```
https://your-api-url.com/jobs
```

### GET /jobs — Query Parameters

| Parameter | Type | Example | Description |
|----------|------|---------|-------------|
| `location` | string | `Remote` | Filter by location |
| `role` | string | `Engineer` | Job title keyword |
| `employment_type` | string | `freelance` | Filter job type |
| `source_api` | string | `adzuna` | Filter by source |
| `skills` | string[] | `["Python"]` | Filter by skills |

### Example Response

```json
[
  {
    "job_id": "adz-87493",
    "title": "Backend Engineer",
    "company": "Techie Inc.",
    "location": "Remote",
    "salary_info": "$100k–$120k",
    "employment_type": "full-time",
    "posted_date": "2025-03-29",
    "skills_required": ["Python", "Django", "SQL"],
    "source_api": "adzuna"
  }
]
```



<!-- ## 📊 Analytical Use Cases

With the integrated dataset in BigQuery, we can explore:

- Job **trends by location** or **job type**
- **Salary** insights for similar roles across platforms
- **Skill demand** across different industries
- Comparison: **Freelance vs Full-time** opportunities



## 🔮 Future Enhancements

- ✅ Add **data validation and anomaly detection**
- 🧠 Perform **sentiment analysis** on job descriptions
- 📈 Build a **dashboard** in Looker Studio for recruiters
- 🌍 Add more regional or international job boards
- 🛡️ Implement **OAuth or API key protection** -->



## 🧑‍💻 Technologies Used

- **Google Cloud Platform**
  - Cloud Composer (Airflow)
  - Cloud Storage
  - BigQuery
- **Python**
  - `requests`, `pandas`, `datetime`
- **APIs**
  - The Muse, Adzuna, Hacker News
<!-- - FastAPI or Flask for REST API Layer -->


