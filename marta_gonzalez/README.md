# Environmental Impacts of Weather: Data Integration Project

## Overview

This project explores how weather conditions influence environmental factors such as air quality and water levels. We integrate three publicly accessible datasets to analyze patterns and correlations across Washington, D.C.

---

## Data Sources

### 1. Open-Meteo Weather API
- **Endpoint:** `https://api.open-meteo.com/v1/forecast`
- **Inputs:**
  - `latitude`, `longitude`
  - `hourly`: e.g., `temperature_2m`, `precipitation`, `wind_speed_10m`
- **Expected Output:**
  - JSON with hourly weather forecasts (temperature, wind speed, rain, etc.)

### 2. NYC Open Data – Air Quality
- **Endpoint:** `https://data.cityofnewyork.us/resource/c3uy-2p5r.json`
- **Inputs:** Optional `date`, `borough`
- **Expected Output:**
  - JSON with air quality metrics (PM2.5, AQI, etc.) by borough and date.

### 3. USGS Water Services API
- **Endpoint:** `https://waterservices.usgs.gov/nwis/iv/`
- **Inputs:**
  - `site` (location/station ID)
  - `parameterCd` (e.g., 00060 for streamflow)
  - `format=json`
- **Expected Output:**
  - Real-time streamflow or water level data from a specific station.

---

## Goals

- Compare weather and precipitation data with local air quality.
- Assess how rainfall and temperature correlate with water level changes.
