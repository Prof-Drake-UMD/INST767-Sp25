# Gun Violence Data Integration Project

## Overview

This project focuses on building an end-to-end data pipeline using Google Cloud Platform on gun violence–related data from three major U.S. cities of Chicago, Washington D.C. and New York City. It aims to study trends on firearm-related incidents in every city. 

## API Data Sources

Below are the three public APIs selected for this project. These APIs offer incident-level gun violence data and are updated regularly.

---

### 1. **Washington, D.C. Gun Violence Data**
- **API Type:** ArcGIS REST (GeoJSON)
- **Endpoint:**  
  [https://opendata.arcgis.com/datasets/89bfd2aed9a142249225a638448a5276_29.geojson](https://opendata.arcgis.com/datasets/89bfd2aed9a142249225a638448a5276_29.geojson)
- **Inputs:** None (full GeoJSON can be fetched directly)
- **Expected Data Fields:**
  - `offense` (e.g., Assault w/ Gun)
  - `method` (e.g., firearm)
  - `block_group`, `neighborhood_cluster`
  - `report_date`, `clearance_date`
  - `latitude`, `longitude`

---

### 2. **CDC Firearm Mortality by State **

- **Endpoint:**  
  [`https://data.cdc.gov/resource/fpsi-y8tj.json`](https://data.cdc.gov/resource/fpsi-y8tj.json)

- **Inputs:**  
  - `$where`: `intent LIKE 'FA_%'`  
    *(Filters for firearm-related categories such as `FA_Suicide`, `FA_Homicide`, and `FA_Deaths`)*
  - `$limit`: `1000` *(Adjustable based on volume needs)*

- **Expected Fields:**  
  - `name`: Name of the U.S. state  
  - `geoid`: State-level geographic FIPS code  
  - `intent`: Firearm-specific cause of death (e.g., `FA_Homicide`)  
  - `period`: Reporting year (used for `incident_date`)  
  - `count_sup`: Whether values were suppressed (`true/false`)  
  - `rate`: Crude mortality rate  
  - `date_as_of`: Last update timestamp from the CDC  
  - `ttm_date_range`: Trailing 12-month reporting window

---

### 3. **NYPD Complaint Data (New York City)**
- **API Type:** Socrata Open Data API (SODA)
- **Endpoint:**  
  [https://data.cityofnewyork.us/resource/5uac-w243.json](https://data.cityofnewyork.us/resource/5uac-w243.json)
- **Inputs:**
  - `$where`: `pd_desc LIKE '%FIREARM%'`
  - `$limit`: 1000
- **Expected Data Fields:**
  - `cmplnt_fr_dt`, `boro_nm`, `law_cat_cd`
  - `ofns_desc`, `pd_desc` (includes firearm type)
  - `latitude`, `longitude`, `juris_desc`

---

Which neighborhoods in D.C. and boroughs in NYC report the most firearm-related incidents?
How have firearm-related incidents trended over the past 12 months in each city?
How do firearm-related injury rates vary across states, according to the CDC?
Is there any correlation between urban gun violence (DC/NYC) and state-level firearm death rates (CDC)?