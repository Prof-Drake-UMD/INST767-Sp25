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
  - JSON with hourly weather forecasts (temperature, wind speed, precipitation, etc.)
- **Use:** Analyze how temperature and precipitation vary by hour.

### 2. Open-Meteo Air Quality API
- **Endpoint:** `https://air-quality-api.open-meteo.com/v1/air-quality`
- **Inputs:**
  - `latitude`, `longitude`
  - `hourly`: e.g., `pm10`, `pm2_5`, `ozone`, `nitrogen_dioxide`, `sulphur_dioxide`
  - `timezone`
- **Expected Output:**
  - Hourly pollutant concentrations for PM2.5, PM10, CO, NO₂, O₃, and SO₂.
- **Use:** Monitor hourly changes in air pollution levels and compare with weather conditions.

### 3. USGS Water Services API
- **Endpoint:** `https://waterservices.usgs.gov/nwis/iv/`
- **Inputs:**
  - `site` (station ID for a DC river gauge)
  - `parameterCd` (e.g., 00060 for streamflow)
  - `format=json`
- **Expected Output:**
  - Real-time streamflow or water level data from a specified monitoring station.
- **Use:** Analyze changes in water flow and correlate with recent weather events (e.g., rainfall).

---

## Goals

- Compare weather and precipitation data with air quality indicators.
- Assess how rainfall and temperature correlate with changes in streamflow.
- Evaluate the short-term environmental impact of weather in Washington, D.C.

