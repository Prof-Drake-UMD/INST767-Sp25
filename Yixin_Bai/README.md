# Project: Transit Data Integration Pipeline on GCP

## 🎯 Objective

The primary objective is to:
- Extract real-time transit data from three distinct public APIs
- Transform the data into a standardized format
- Load the processed data into Google BigQuery
- Create an integrated view for comprehensive transit analysis
- Automate the entire workflow using Apache Airflow

## 🌐 Selected APIs and Justification

For this project, three public transit APIs providing real-time operational data were chosen:

| API | Description | Data Type |
|-----|-------------|-----------|
| MBTA API (v3) | Massachusetts Bay Transportation Authority | Routes & Predictions |
| WMATA API | Washington Metropolitan Area Transit Authority | Rail Predictions |
| CTA API | Chicago Transit Authority Train Tracker | Train Positions |

---

### 1️⃣ MBTA API v3 – Routes & Real-Time Predictions

📌 **Why this API?**

* Provides both static route information (names, colors, types) and dynamic, real-time arrival/departure predictions for specified routes
* Offers a structured JSON:API format
* Helps understand current service status and expected wait times on the MBTA system

**Endpoints Used:**

* **Get Routes:** `https://api-v3.mbta.com/routes`
    * **Inputs:** None (fetches all routes)
    * **Expected Output (Key Fields):**
    ```json
    {
      "data": [
        {
          "id": "Red", // route_id
          "attributes": {
            "color": "DA291C", // route_color
            "description": "Rapid Transit", // route_description
            "long_name": "Red Line", // route_name
            "short_name": "", // route_short_name
            "text_color": "FFFFFF", // route_text_color
            "type": 1 // route_type (Subway)
          }
        }
      ]
    }
    ```

* **Get Predictions:** `https://api-v3.mbta.com/predictions`
    * **Inputs:**
        * `filter[route]`: (string, comma-separated) e.g., `Red,Green-B,Green-C` (Required filter)
    * **Expected Output (Key Fields):**
    ```json
    {
      "data": [
        {
          "id": "prediction-70061-70001-Red-1714751400", // prediction_id
          "attributes": {
            "arrival_time": "2025-05-03T16:00:00-04:00", // arrival_time (ISO Format)
            "departure_time": "2025-05-03T16:00:15-04:00", // departure_time (ISO Format)
            "direction_id": 0, // direction_id
            "status": "Approaching" // status
          },
          "relationships": {
            "route": { "data": { "id": "Red" } }, // route_id
            "stop": { "data": { "id": "70061" } }, // stop_id
            "vehicle": { "data": { "id": "1905" } } // vehicle_id (if available)
          }
        }
      ]
    }
    ```

---

### 2️⃣ WMATA API – Real-Time Rail Predictions

📌 **Why this API?**

* Provides system-wide real-time arrival predictions for Metrorail stations
* Data includes destination, line color, and minutes until arrival ('Min' field requires special handling: 'ARR', 'BRD', or number)
* Complements MBTA/CTA by adding data from a different major transit system

**Endpoint Used:**

* **Get Rail Predictions:** `https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All`
    * **Inputs:** `api_key` (header)
    * **Expected Output (Key Fields):**
    ```json
    {
      "Trains": [
        {
          "Car": "6", // cars
          "Destination": "Wiehle",
          "DestinationCode": "N06", // destination_code
          "DestinationName": "Wiehle-Reston East", // destination_name
          "Group": "2",
          "Line": "SV", // train_line (Route ID)
          "LocationCode": "A01", // station_code
          "LocationName": "Metro Center", // station_name
          "Min": "3" // minutes_to_arrival (String: number, 'ARR', 'BRD')
        }
      ]
    }
    ```

---

### 3️⃣ CTA Train Tracker API – Real-Time Train Positions

📌 **Why this API?**

* Provides the real-time geographic location (latitude, longitude) and heading of active trains
* Includes information about the train's next stop and predicted arrival time there
* Offers a different perspective (physical location) compared to station-based predictions from MBTA/WMATA

**Endpoint Used:**

* **Get Train Positions:** `http://www.transitchicago.com/api/1.0/ttpositions.aspx`
    * **Inputs:**
        * `key`: (string) Your CTA API key
        * `rt`: (string, comma-separated) Route identifiers (e.g., `Red,Blue,Brn`). Required
        * `outputType`: `JSON`
    * **Expected Output (Key Fields under `ctatt.route[...].train[...]`):**
    ```json
    {
      "rn": "411", // vehicle_id (run number)
      "destSt": "30173",
      "destNm": "Howard", // destination_name
      "trDr": "1", // direction
      "nextStaId": "40900", // next_station_id
      "nextStaNm": "Sheridan", // next_station_name
      "nextArrT": "20250503 15:05:22", // predicted_arrival_time (String: YYYYMMDD HH:MM:SS)
      "isApp": "0", // is_approaching
      "isDly": "0", // is_delayed
      "lat": "41.94765", // latitude
      "lon": "-87.65353", // longitude
      "heading": "358" // heading
    }
    ```

---

## 📊 Sample Integrated Data (BigQuery)

Below is a sample of the integrated data schema in BigQuery, showing records from all three transit systems in a unified format:

| Column | Type | Description |
|--------|------|-------------|
| transit_system | STRING | Source transit system (MBTA, WMATA, CTA) |
| route_id | STRING | Transit line identifier |
| station_id | STRING | Station/stop identifier |
| station_name | STRING | Name of the station/stop |
| vehicle_id | STRING | Vehicle/train identifier |
| direction_id | FLOAT | Direction of travel (typically 0 or 1) |
| arrival_time | STRING | Predicted arrival time |
| departure_time | STRING | Predicted departure time |
| status | STRING | Current status of vehicle (e.g., "Approaching") |
| prediction_id | STRING | Unique identifier for prediction |
| latitude | STRING | Geographic latitude of vehicle |
| longitude | STRING | Geographic longitude of vehicle |
| heading | STRING | Compass heading of vehicle (degrees) |
| data_type | STRING | Type of transit data |
| recorded_at | STRING | Timestamp when data was collected |


## 🔄 Modifications and Challenges

Throughout the development process, several technical challenges were encountered and overcome:

- **Cloud Composer Permission Configuration**: Ensured the Airflow service account had sufficient permissions, particularly `storage.buckets.create` and `roles/storage.admin` role
- **GCS Bucket Naming**: Addressed global uniqueness requirements, ensuring bucket names complied with the 3-63 character limitation
- **Project ID Configuration Error**: Fixed the `Unknown project id: 767project` error by correctly distinguishing between project names and project IDs
- **DAG Import Error**: Troubleshooted and fixed incorrect parameter passing to `BigQueryCreateEmptyDatasetOperator` (such as `storage_class='STANDARD'`)
- **API Request Parameter Configuration**: Resolved `400 Bad Request` errors with the MBTA API `/predictions` endpoint by adding required filter parameters
- **Data Type Mismatches**: Resolved schema incompatibilities in BigQuery tables, particularly with fields like `route_description`
- **Data Standardization**: Created a unified schema to integrate data structures from three different transit systems
- **Error Handling Mechanisms**: Implemented robust error catching and logging to ensure pipeline resilience during API failures
