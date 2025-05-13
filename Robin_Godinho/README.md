
# 📰 Real-Time News Sentiment & Trends Tracker

## 📘 Overview

This project is a real-time data pipeline that collects news headlines from multiple public APIs, cleans and standardizes the content, performs sentiment analysis, and stores the results in **Google BigQuery** for visualization or future analysis.

Built using Python and Google Cloud Platform (GCP), the system follows a DAG-style ETL pipeline structure:  
**Ingest → Transform → Load → Store**

---

## 🎯 Objective

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

## 🧱 Project Structure

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

## 🔁 Pipeline Workflow

1. **Ingest**  
   Pulls the latest news headlines/articles from the selected APIs.

2. **Transform**  
   Cleans text, removes duplicates, formats dates, and extracts metadata.

3. **Sentiment Analysis**  
   Uses `TextBlob` or `VADER` to classify sentiment: Positive, Neutral, or Negative.

4. **Load to BigQuery**  
   Writes the processed and labeled data to a BigQuery table.

---
## Architecture
+--------------------+        +----------------------+        +-------------------------+        +-----------------------------+
|  Cloud Scheduler   | ───▶   |  Publisher Function  | ───▶   |     Pub/Sub Topic       | ───▶   | Subscriber Function         |
|  (Triggers timed)  |        |  (sends metadata)    |        |  (real-news-ingest)     |        | + Loads to BigQuery         |
+--------------------+        +----------------------+        +-------------------------+        | Dataset: real_news_data     |
                                                                                                 | Tables: gnews, mediastack,  |
                                                                                                 |          newsdata           |
                                                                                                 +-----------------------------+


## ⚙️ Technologies Used

- Python 3.10+
- Google Cloud Platform (BigQuery, Cloud Storage)
- Apache Airflow (optional for DAG orchestration)
- pandas, requests, TextBlob/VADER


## 🙌 Author

**Robin Godinho**  
Graduate Student in Information Management  
University of Maryland, College Park  
Project for INST767: Big Data Infrastrcuture  
Spring 2025
