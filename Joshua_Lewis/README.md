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



---



