# INST767 Final Project: Nutrition-Exercise Data Pipeline

## Project Overview

This project implements a cloud-ready data pipeline that integrates **three public APIs** to collect and process structured data about food, exercise, and nutritional metadata. The pipeline is designed for extensibility, asynchronous processing, and compatibility with Google BigQuery.

---

## Project Goals

- Integrate **three continuously updated public APIs**
- Extract and transform real-world food and exercise metadata
- Create three robust tables suitable for analysis in BigQuery
- Support meaningful, cross-domain SQL queries

---

## APIs Used

| API                 | Description                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| **Nutritionix**       | Real-time food nutrient breakdowns for natural queries (e.g., "1 apple")     |
| **Wger**              | Exercise metadata, including muscles targeted, categories, equipment        |
| **USDA FoodData Central** | Verified nutrient and ingredient data from a federally maintained database |

---

## Output Tables

| Table Name         | Description                                                  |
|--------------------|--------------------------------------------------------------|
| `nutrition_logs`   | Contains nutrition info from Nutritionix based on real-world ingredients |
| `exercises`        | Exercise metadata from Wger including category, muscles, and equipment |
| `usda_foods`       | USDA food product entries with nutrient values and food groups |

---

## Pipeline Components

Sai_Gangineni/
├── dag/
│ ├── api_calls.py
│ ├── transform_nutrition.py
│ ├── transform_exercises.py
│ ├── transform_usda.py
│ └── pipeline.py
├── output/
│ ├── nutrition_logs.csv
│ ├── exercises.csv
│ └── usda_foods.csv

---

## Sample SQL Use Cases

- Match USDA food groups with high-protein, low-fat entries
- Join Nutritionix and USDA data to compare branded vs. generic food items
- Recommend exercises based on nutrient-supporting muscle recovery (e.g., potassium)
