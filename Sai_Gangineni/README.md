# Fitness & Nutrition Data Pipeline

## Overview

This project builds a **cloud-native data pipeline** that integrates data from three public APIs — Wger (exercise), Nutritionix (food nutrition), and USDA (food metadata). The goal is to enable real-time, cross-domain insights about food and fitness.

It was developed as a final project for **INST767** (Spring 2025).

---

## Features

- **Fully serverless pipeline** using Google Cloud Functions, Pub/Sub, BigQuery, and Cloud Scheduler  
- Nutrition data via **Nutritionix**  
- Exercise data via **Wger**  
- Food metadata via **USDA FoodData Central**  
- Final data stored in BigQuery for analysis and dashboarding  
- Scheduled daily ingestion using **Cloud Scheduler**

---

## Architecture

```
+----------------+           +------------------+
| Cloud Scheduler|  ───────► | Publisher Function|
+----------------+           +------------------+
                                  │
                                  ▼
                          +------------------+
                          |   Pub/Sub Topic   |
                          +------------------+
                                  │
                                  ▼
                        +----------------------+
                        | Subscriber Function  |
                        | + BigQuery Insert    |
                        +----------------------+
```

- **Topics**: `usda-topic`, `nutrition-topic`, `wger-topic`  
- **Functions**: Each API has a publisher/subscriber pair  
- **Storage**: BigQuery dataset `inst767_final` with 3 tables


---

## Queries

Five real-world queries were created and tested to generate cross-API insights. These include:

1. Top protein-per-calorie foods  
2. Exercises matching food types like chicken  
3. Food logs matched to USDA metadata  
4. Popular equipment types in exercises  
5. Full cross-table join of USDA → Nutritionix → Wger  

See [`sample_queries.sql`](./sample_queries.sql) for the query code.

---

## Automation via Cloud Scheduler

All three publishers are triggered daily using **Cloud Scheduler**:

| Job Name         | Schedule (ET) | Triggers                      |
|------------------|---------------|-------------------------------|
| `usda-job`       | 9:00 AM        | USDA → Pub/Sub                |
| `nutritionix-job`| 9:30 AM        | Nutritionix via USDA matches |
| `wger-job`       | 10:00 AM       | Wger exercise ingestion       |

---

## Final Notes

- All deployment is serverless — no compute instances used  
- The data grows over time as Cloud Scheduler triggers fetch new data  
- Screenshots and logs of successful ingestion and queries were captured for project documentation

---

## Credits

Developed by **Sai Gangineni**  
University of Maryland, College Park  
Spring 2025 – INST767 Final Project
