# Sustainability Analytics Pipeline

## Project Overview

My project aims to implement a data pipeline that ingests, transforms, and analyzes sustainability-related data from multiple APIs. The goal is to collect carbon emissions data, electricity production information, and weather patterns. This data can be used in the future to provide insights into environmental impacts and help identify opportunities for reducing carbon footprints.

## Data Sources

The pipeline will utilize the following APIs:

### 1. Carbon Interface API

- Provides carbon emissions data for various activities and energy sources.
- Offers estimates for electricity usage, transportation, and more.
- **Link**: [Carbon Interface](https://www.carboninterface.com/)

### 2. ElectricityMap API

- Shows real-time electricity carbon intensity by region.
- Provides data on energy production sources.
- **Link**: [ElectricityMap](https://www.electricitymaps.com/)

### 3. WeatherBit API

- Detailed weather data including temperature, precipitation, wind.
- Historical and forecast capabilities.
- **Link**: [WeatherBit](https://www.weatherbit.io/)

## Data from API

The free-tier version of the ElectricityMap API allows for data retrieval from only one zone. For this project, the zone **"US-MIDA-PJM"** was selected. This zone covers the states of **District of Columbia, Maryland, Virginia, West Virginia, Pennsylvania, New Jersey, Delaware, Ohio, and Kentucky**.

<p align="center">
  <img src="zone_map.png" alt="US-MIDA-PJM Zone Map" width="700">
</p>

### Data Types from APIs

#### **Carbon Interface API**

The Carbon Interface API provides estimated carbon emissions for electricity consumption in different states. The key attributes include:

- **electricity_unit**: Measurement unit .
- **electricity_value**: The amount of electricity used.
- **carbon_g, carbon_lb, carbon_kg, carbon_mt**: Carbon emissions in grams, pounds, kilograms, and metric tons.
- **estimated_at**: Timestamp of the estimate.

#### **ElectricityMap API**

The ElectricityMap API provides various breakdowns of electricity data, including:

- **Power Consumption Breakdown**: Nuclear, geothermal, biomass, coal, wind, solar, hydro, gas, oil, unknown.
- **Power Production Breakdown**: Similar categories as consumption.
- **Power Import/Export Breakdown**: Tracks electricity flow between regions.
- **Fossil-Free & Renewable Percentage**: Measures the proportion of energy from clean sources.
- **Total Consumption & Production**: Aggregated values for energy usage and generation.
- **Estimation Method**: Indicates how the data is estimated.

#### **WeatherBit API**

The WeatherBit API provides real-time and historical weather data, including:

- **Temperature (temp, app_temp)**: Actual and perceived temperature.
- **Air Quality Index (aqi)**: Measurement of air pollution.
- **Cloud Cover (clouds)**: Percentage of cloud coverage.
- **Dew Point (dewpt)**: Temperature at which condensation forms.
- **Wind (wind_spd, wind_dir, wind_cdir, wind_cdir_full)**: Speed and direction of wind.
- **Precipitation (precip, snow)**: Rain and snow levels.
- **Pressure (pres, slp)**: Atmospheric pressure.
- **Humidity (rh)**: Relative humidity percentage.
- **Solar Radiation (solar_rad, ghi, dhi, dni, elev_angle)**: Measures of solar exposure.
- **Visibility (vis)**: Distance of clear sight.
- **Weather Description (weather.description, weather.code)**: Text and code representation of weather conditions.
- **Time Information (ob_time, datetime, sunrise, sunset, timezone)**: Various timestamps and location data.

## Data Model Design

Since BigQuery is a columnar, non-relational database, a denormalized data model has been designed, consisting of three tables:

### 1. carbon_emissions Table

Stores carbon emissions estimates for each state.

| Column Name       | Data Type | Description                            |
| ----------------- | --------- | -------------------------------------- |
| id                | STRING    | Unique identifier for the estimate     |
| state             | STRING    | State code (e.g., "VA")                |
| estimated_at      | TIMESTAMP | Timestamp of estimate                  |
| electricity_value | FLOAT     | Amount of electricity used             |
| electricity_unit  | STRING    | Unit of electricity used (e.g., "mwh") |
| carbon_g          | FLOAT     | Carbon emissions in grams              |
| carbon_kg         | FLOAT     | Carbon emissions in kilograms          |
| carbon_lb         | FLOAT     | Carbon emissions in pounds             |
| carbon_mt         | FLOAT     | Carbon emissions in metric tons        |

### 2 electricity_production Table

Stores electricity generation and consumption data by zone.

| Column Name            | Data Type | Description                     |
| ---------------------- | --------- | ------------------------------- |
| date                   | DATE      | Date of data collection         |
| timestamp              | TIMESTAMP | Time of data collection         |
| zone                   | STRING    | Electricity zone                |
| fossil_free_percentage | FLOAT64   | Percentage of fossil-free power |
| renewable_percentage   | FLOAT64   | Percentage of renewable power   |
| total_production       | FLOAT64   | Total power production (MW)     |
| total_consumption      | FLOAT64   | Total power consumption (MW)    |
| carbon_intensity       | FLOAT64   | Carbon intensity                |
| carbon_intensity_unit  | STRING    | Unit of carbon intensity        |
| production_nuclear     | FLOAT64   | Power from nuclear (MW)         |
| production_geothermal  | FLOAT64   | Power from geothermal (MW)      |
| production_biomass     | FLOAT64   | Power from biomass (MW)         |
| production_coal        | FLOAT64   | Power from coal (MW)            |
| production_wind        | FLOAT64   | Power from wind (MW)            |
| production_solar       | FLOAT64   | Power from solar (MW)           |
| production_hydro       | FLOAT64   | Power from hydro (MW)           |
| production_gas         | FLOAT64   | Power from gas (MW)             |
| production_oil         | FLOAT64   | Power from oil (MW)             |
| production_unknown     | FLOAT64   | Power from unknown sources (MW) |
| estimation_method      | STRING    | Method used for estimation      |
| region                 | STRING    | Region of data collection       |

### 3. weather_data Table

Stores real-time weather information for the major cities in each state.

| Column Name   | Data Type | Description             |
| ------------- | --------- | ----------------------- |
| city          | STRING    | City name               |
| state         | STRING    | State code              |
| timestamp     | TIMESTAMP | Time of data collection |
| temp_c        | FLOAT     | Temperature (Celsius)   |
| wind_speed    | FLOAT     | Wind speed (m/s)        |
| humidity      | FLOAT     | Humidity percentage     |
| cloud_cover   | FLOAT     | Cloud cover percentage  |
| precipitation | FLOAT     | Rainfall (mm)           |
| solar_rad     | FLOAT     | Solar radiation (W/m²)  |
