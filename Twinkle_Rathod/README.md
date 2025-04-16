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

### 2. **Chicago Crime Incidents (2001–Present)**
- **API Type:** Socrata Open Data API (SODA)
- **Endpoint:**  
  [https://data.cityofchicago.org/resource/ijzp-q8t2.json](https://data.cityofchicago.org/resource/ijzp-q8t2.json)
- **Inputs:**
  - `$where`: `primary_type = 'HOMICIDE' AND description LIKE '%HANDGUN%'`
  - `$limit`: max 1000 per request (pagination recommended)
- **Expected Data Fields:**
  - `date`, `primary_type`, `description`
  - `location_description`, `block`
  - `latitude`, `longitude`, `community_area`
  - `arrest` (boolean)

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
Which neighborhoods or community areas in each city report the highest number of gun violence incidents?
How have firearm-related incidents trended over the past 12 months in each city?
What proportion of gun-related incidents result in an arrest? (chicagoo, nyc)