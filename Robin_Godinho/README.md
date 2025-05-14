
# Real-Time News Sentiment & Trends Tracker

## Overview

This project is a real-time data pipeline that collects news headlines from multiple public APIs, cleans and standardizes the content, performs sentiment analysis, and stores the results in **Google BigQuery** for visualization or future analysis.

Built using Python and Google Cloud Platform (GCP), the system follows a DAG-style ETL pipeline structure:  
**Ingest → Transform → Load → Store**

---

## Objective

The goal is to:
- Integrate at least **three different news APIs**
- Build a system that updates regularly using scheduling (e.g., Apache Airflow)
- Perform **sentiment analysis** on the headlines/articles
- Store processed data in **Google BigQuery**
- Optionally visualize trends using **Looker Studio**

---

## 🌐 Public APIs Used

| API          | Description                                  | Free Tier |
|--------------|----------------------------------------------|-----------|
| [NewsData.io](https://newsdata.io/) | Global and regional news data              | ✅ |
| [GNews](https://gnews.io/)          | Aggregates global news headlines           | ✅ |
| [Media Stack](https://mediastack.com/) | International news coverage| ✅ |

---

## Project Structure

```
real_time_news_sentiment/
├── dags/
│   └── news_sentiment_dag.py      # Optional: Airflow DAG
├── data/
│   └── raw/                       # Raw data from APIs
├── utils/
│   ├── ingest.py              # Ingest data
│   ├── clean_data.py              # Clean and standardize
│   └── sentiment_analysis.py      # Add sentiment scores
├── README.md
└── requirements.txt
```

---

## Pipeline Workflow

1. **Publish**  
   A message containing the file path and destination table is sent to the Pub/Sub topic (`real-news-ingest`).

2. **Subscribe & Extract**  
   The subscriber script (`subscriber_loader.py`) receives the message and reads the specified CSV file.

3. **Transform**  
   The subscriber handles light preprocessing (e.g., auto-detect schema, skip headers) before loading.

4. **Load to BigQuery**  
   The subscriber writes the contents of the CSV to the specified BigQuery table within the `real_news_data` dataset.

---

## Architecture Overview

```
+--------------------+
|  Cloud Scheduler   |
| (Triggers on cron) |
+--------------------+
          │
          ▼
+----------------------+
|  Publisher Function  |
| (Publishes metadata) |
+----------------------+
          │
          ▼
+-------------------------+
|     Pub/Sub Topic       |
|   (real-news-ingest)    |
+-------------------------+
          │
          ▼
+-----------------------------+
|  Subscriber Function        |
|  (subscriber_loader.py)     |
|  + Loads to BigQuery        |
+-----------------------------+
          │
          ▼
+-----------------------------+
| BigQuery Dataset:          |
| real_news_data             |
| Tables:                    |
|  - gnews                   |
|  - mediastack              |
|  - newsdata                |
+-----------------------------+
```

## Workflow Summary

- **Cloud Scheduler** triggers the publisher periodically  
- **Publisher Function** sends file + table info to the topic  
- **Pub/Sub** queues the message  
- **Subscriber Function** receives the message and loads the data into BigQuery  
- **BigQuery** stores it in the `real_news_data` dataset

## ⚙️ Technologies Used

- Google Cloud Pub/Sub  
- Google Cloud Functions  
- Google BigQuery  
- Cloud Scheduler  
- Python (Pub/Sub Subscriber)
                        
## Author

**Robin Godinho**  
Graduate Student in Information Management  
University of Maryland, College Park  
Project for INST767: Big Data Infrastrcuture  
Spring 2025
