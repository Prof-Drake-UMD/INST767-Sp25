# **Food Truck Location Optimizer**

## **Overview**
The **Food Truck Location Optimizer** is a data pipeline project designed to help food truck owners identify the best locations to operate. By analyzing **local events**, **weather conditions**, and **competition**, the project provides actionable insights to maximize customer foot traffic and sales potential.

The pipeline integrates data from three public APIs:
1. **Ticketmaster API**: To fetch information about local events that attract large crowds.
2. **Yelp API**: To gather information about nearby restaurants and food trucks for competition analysis.
3. **OpenWeatherMap API**: To obtain current weather conditions that influence customer turnout.

The project processes, transforms, and loads the data into **Google BigQuery**, enabling advanced analytics and visualization.

---

## **Folder Structure**
![image](https://github.com/user-attachments/assets/e8613c17-4f43-41c0-8b16-6e67c9ebc9f5)

---

## **APIs Used**

### 1. **Ticketmaster API**
- **Purpose**: Fetches information about events happening in a specific city.
- **Endpoint**: `https://app.ticketmaster.com/discovery/v2/events.json`
- **Key Parameters**:
  - `city`: The city to search for events.
  - `apikey`: Your Ticketmaster API key.
- **Data Extracted**:
  - Event name, date, time, venue name, and venue location (latitude, longitude).

### 2. **Yelp API**
- **Purpose**: Retrieves information about nearby restaurants and food trucks for competition analysis.
- **Endpoint**: `https://api.yelp.com/v3/businesses/search`
- **Key Parameters**:
  - `location`: The city or area to search.
  - `categories`: Filter by business categories (e.g., food trucks, restaurants).
  - `sort_by`: Sort results by rating, distance, or review count.
- **Data Extracted**:
  - Business name, rating, review count, and location (latitude, longitude).

### 3. **OpenWeatherMap API**
- **Purpose**: Provides current weather data for the target city.
- **Endpoint**: `http://api.openweathermap.org/data/2.5/weather`
- **Key Parameters**:
  - `q`: City name for weather data.
  - `appid`: Your OpenWeatherMap API key.
  - `units`: Specify units (metric or imperial).
- **Data Extracted**:
  - Temperature, weather condition (e.g., sunny, rainy), and wind speed.

---

## **BigQuery Tables**
The pipeline loads data into the following BigQuery tables:

### **1. Events Table**
- **Table Name**: `Location_Optimizer_Analytics.events`
- **Schema**:
  - `event_name`: STRING
  - `postalCode`: STRING
  - `startDateTime`: TIMESTAMP
  - `endDateTime`: TIMESTAMP
  - `venue_name`: STRING
  - `latitude`: FLOAT
  - `longitude`: FLOAT

### **2. Business Table**
- **Table Name**: `Location_Optimizer_Analytics.business`
- **Schema**:
  - `business_name`: STRING
  - `zip_code`: STRING
  - `rating`: FLOAT
  - `review_count`: INTEGER
  - `categories`: STRING
  - `latitude`: FLOAT
  - `longitude`: FLOAT

### **3. Weather Table**
- **Table Name**: `Location_Optimizer_Analytics.weather`
- **Schema**:
  - `city`: STRING
  - `temperature`: FLOAT
  - `weather_condition`: STRING
  - `humidity`: INTEGER
  - `wind_speed`: FLOAT
  - `timestamp`: TIMESTAMP

---

## **Technologies Used**
- **Python**
- **Google BigQuery**
- **BigQuery Views**
- **Google Cloud Platform (GCP)**
- **Google Cloud Pub/Sub**

---
## **SQL Query Descriptions**

### **Query 1: Top 5 ZIPs by Event & Business Volume**
- **Description**: This query identifies the top 5 ZIP codes with the highest number of events and businesses. It also calculates the average business rating for each ZIP code.

---

### **Query 2: Opportunity Score = Events × Avg Business Rating**
- **Description**: This query calculates an "opportunity score" for each ZIP code by multiplying the number of events by the average business rating. The top 5 ZIP codes with the highest opportunity scores are returned.

---

### **Query 3: Business Categories vs. Event Count in All ZIPs**
- **Description**: This query analyzes the relationship between business categories and event counts across all ZIP codes. It identifies the number of businesses and events associated with each category.

---

### **Query 4: Top Categories by Avg Rating (min 5 businesses)**
- **Description**: This query identifies the top business categories based on their average ratings. Only categories with at least 5 businesses are included, and the results are sorted by average rating.

---

### **Query 5: Top Categories by Avg Review Count (min 3 businesses)**
- **Description**: This query identifies the top business categories based on their average review count. Only categories with at least 3 businesses are included, and the results are sorted by average review count.

---

