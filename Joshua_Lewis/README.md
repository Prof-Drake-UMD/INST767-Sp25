# Joshua_Lewis_README

## Purpose 
The data used in this project comes from three specific APIs, each contributing a unique layer of detail. The project is designed to ingest and combine this data to generate meaningful insights about natural disasters. My primary focus is to examine the conditions at a location prior to a disaster event. I used Ambee’s free-tier natural disaster API to identify recent disaster occurrences. To enrich this data, I incorporated weather information from the Open-Meteo API to better understand the environmental conditions leading up to each event. Finally, I used the OpenStreetMap Nominatim API for reverse geocoding, translating raw latitude and longitude coordinates into human-readable locations—an essential step for truly understanding where these events occur.



## 🔌 Integrated APIs

### 1. **Open-Meteo API (Weather Data)**
- **Base URL:** `https://api.open-meteo.com/v1/forecast`
- **Inputs:**
  - `latitude` (float)
  - `longitude` (float)
- **Outputs:**
  - `current`: Real-time weather conditions (temperature, humidity, wind speed, etc.)
  - `hourly`: Hour-by-hour forecast including precipitation, temperature, and wind data
- **Format:** JSON, parsed into pandas DataFrames

---

### 2. **OpenStreetMap Nominatim API (Geocoding & Reverse Geocoding)**
- **Base URL:** 
  - Geocoding: `https://nominatim.openstreetmap.org/search`
  - Reverse Geocoding: `https://nominatim.openstreetmap.org/reverse`
- **Inputs:**
  - For geocoding: address string (`q`)
  - For reverse geocoding: `latitude`, `longitude`
- **Outputs:**
  - Latitude & longitude (geocoding)
  - Human-readable address (reverse geocoding)
- **Notes:** Requires a `User-Agent` header per API usage policy

---

### 3.Ambee Natural Disasters API
- **Base URL:** 
Base URL: https://api.ambeedata.com/disasters/latest/by-country-code

Inputs:
countryCode (e.g., "US")

Outputs:
List of disaster events with type, title, description, severity, coordinates, and timestamp

Authorization: Requires API key in the header

Format: JSON

##Issue 5 partial completion
As I progressed through this project, I found Issue #4 to be the most challenging. I struggled with deployment for a significant portion of the project, attempting to use Google Cloud Functions, Cloud Run, and eventually Cloud Composer. These challenges persisted right up to the final stages of the project.

Because of these delays, I was only able to partially complete Issue #5. I was able to break the pipeline into asynchronous steps — first triggering data collection from Ambee, then enriching that data with weather and location metadata, and finally uploading the processed data to BigQuery. I completed the process of  to setting up Cloud Functions with a public HTTP endpoint (generated by my run_pipeline function), then use Cloud Scheduler to ping that endpoint regularly (e.g., every minute).

After the function ran enough times and collected sufficient data, my plan was to add a secondary script that monitored the dataset size or timestamp thresholds before triggering the upload to BigQuery. Unfortunately, I didn’t get to implement this due to repeated environment setup issues.

I believe a major obstacle was trying to manage Cloud Function deployment through the terminal, especially while dealing with .env files, incorrect folder structure in zipped deployments, and environment variable injection. These created frustrating barriers that slowed down my ability to test and iterate.


Issue #6 – Completed Using Local CSV

Despite the deployment struggles, I was able to move forward with Issue #6 using a locally generated CSV file (disaster_dataset.csv). I conducted all analytical queries against that file after uploading it manually to BigQuery.

I’ve included screenshots in the report showing the query results, as well as SQL examples that demonstrate the insights that would have been returned from a working automated pipeline.



---



