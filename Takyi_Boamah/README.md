# EcoFusion Data Pipeline

**EcoFusion** is a data pipeline designed to integrate and analyze sustainability-related datasets from three public APIs:

- ⚡ **EIA State Electricity Profile API** (Electric emissions by fuel type)
- 🌿 **Carbon Interface API** (Carbon estimates per MWh of electricity usage)
- 🌦️ **OpenWeatherMap API** (Real-time weather metrics by state capital)

The goal is to produce a unified, queryable dataset in **BigQuery** for analyzing environmental impacts of energy generation, with weather context.

## 🌐 API Data Sources

### 1. **EIA API - State Electricity Profile**

- **Endpoint:** `https://api.eia.gov/v2/electricity/state-electricity-profiles/emissions-by-state-by-fuel/data`
- **Inputs:**
  - `api_key`: EIA API key
  - `data[]`: Set to `co2-thousand-metric-tons`
  - `sort[0][column]`: `period`
  - `sort[0][direction]`: `desc`
- **Returns:**
  - CO2 emissions (thousand metric tons) by state and fuel type (e.g., Coal, Natural Gas)
  - Year (`period`), state abbreviation, fuel description

### 2. **Carbon Interface API - Emission Estimates**

- **Endpoint:** `https://www.carboninterface.com/api/v1/estimates`
- **Method:** POST
- **Headers:**
  - `Authorization`: Bearer token
  - `Content-Type`: application/json
- **Payload Example:**

```json
{
  "type": "electricity",
  "electricity_unit": "mwh",
  "electricity_value": 42,
  "country": "US",
  "state": "CA"
}
```

- **Returns:**
  - Estimated emissions: grams, pounds, kilograms, metric tons of CO2
  - Includes the electricity value submitted and a timestamp

### 3. **OpenWeatherMap API - Current Weather**

- **Endpoint:** `http://api.openweathermap.org/data/2.5/weather`
- **Inputs:**
  - `q`: City and country code (e.g., `Baltimore,US`)
  - `appid`: OpenWeatherMap API key
  - `units`: `metric` for Celsius output
- **Returns:**
  - Current temperature, humidity, wind speed/direction, visibility, pressure
  - Main weather type (e.g., Clear, Rain), and cloud cover

## 📁 Project Structure

```
/Takyi_Boamah/
│
├── get_data.py                 # Python ingest script for API data collection
├── .env                        # Environment variables for API keys
├── api_results/                # Stores JSON outputs of API calls
├── schema/
│   └── bigquery_schema.sql     # BigQuery table creation statements
├── queries/
│   └── analysis_queries.sql    # Insightful analysis queries
└── README.md                   # Project overview and instructions
```

## 🧱 BigQuery Dataset Design:

| Table                           | Description                                    |
| ------------------------------- | ---------------------------------------------- |
| `weather_snapshot`              | Real-time weather data by state capital        |
| `carbon_emissions`              | Carbon footprint per electricity unit consumed |
| `state_electricity_emissions`   | Emissions by fuel type and state (from EIA)    |
| `state_emission_summary` (VIEW) | Aggregated emissions per state/year            |

### Table: `weather_snapshot`

| Column Name         | Data Type | Description                                |
| ------------------- | --------- | ------------------------------------------ |
| state               | STRING    | US state abbreviation                      |
| city                | STRING    | State capital or representative city       |
| datetime            | TIMESTAMP | Datetime of the weather snapshot           |
| temp_celsius        | FLOAT64   | Current temperature in Celsius             |
| feels_like_celsius  | FLOAT64   | Perceived temperature in Celsius           |
| humidity_percent    | INT64     | Humidity percentage                        |
| pressure_hpa        | INT64     | Atmospheric pressure in hPa                |
| weather_main        | STRING    | Main weather condition (e.g., Clear, Rain) |
| weather_description | STRING    | Detailed weather description               |
| wind_speed_mps      | FLOAT64   | Wind speed in meters per second            |
| wind_deg            | INT64     | Wind direction in degrees                  |
| cloud_percent       | INT64     | Cloudiness percentage                      |
| visibility_m        | INT64     | Visibility in meters                       |

### Table: `carbon_emissions`

| Column Name           | Data Type | Description                       |
| --------------------- | --------- | --------------------------------- |
| state                 | STRING    | US state abbreviation             |
| country               | STRING    | Country code (typically 'US')     |
| estimated_at          | TIMESTAMP | Time of the estimate              |
| electricity_value_mwh | FLOAT64   | Amount of electricity used in MWh |
| carbon_g              | INT64     | Carbon emitted in grams           |
| carbon_lb             | FLOAT64   | Carbon emitted in pounds          |
| carbon_kg             | FLOAT64   | Carbon emitted in kilograms       |
| carbon_mt             | FLOAT64   | Carbon emitted in metric tons     |

### Table: `state_electricity_emissions`

| Column Name      | Data Type | Description                           |
| ---------------- | --------- | ------------------------------------- |
| state            | STRING    | US state abbreviation                 |
| year             | INT64     | Reporting year                        |
| fuel_type        | STRING    | Fuel ID code (e.g., COL, NG, PET)     |
| fuel_description | STRING    | Full description of the fuel          |
| co2_thousand_mt  | FLOAT64   | CO2 emissions in thousand metric tons |

### View: `state_emission_summary`

| Column Name         | Data Type | Description                           |
| ------------------- | --------- | ------------------------------------- |
| state               | STRING    | US state abbreviation                 |
| year                | INT64     | Reporting year                        |
| coal_emissions      | FLOAT64   | Emissions from coal                   |
| gas_emissions       | FLOAT64   | Emissions from natural gas            |
| petroleum_emissions | FLOAT64   | Emissions from petroleum              |
| other_emissions     | FLOAT64   | Emissions from other sources          |
| total_emissions     | FLOAT64   | Total emissions across all fuel types |

## 🔍 Interesting Analytical Queries

### 1. Do hotter states emit less carbon per MWh on average? 🌡️

```sql
SELECT
  w.state,
  AVG(w.temp_celsius) AS avg_temp,
  AVG(c.carbon_kg / c.electricity_value_mwh) AS kg_per_mwh
FROM `ecofusion.weather_snapshot` w
JOIN `ecofusion.carbon_emissions` c ON LOWER(w.state) = LOWER(c.state)
GROUP BY w.state
ORDER BY kg_per_mwh DESC;
```

### 2. Are clear sky days associated with lower emissions per state? ☀️

```sql
SELECT
  w.state,
  COUNTIF(w.weather_main = 'Clear') AS clear_days,
  AVG(c.carbon_kg) AS avg_emission_all_days,
  AVG(CASE WHEN w.weather_main = 'Clear' THEN c.carbon_kg END) AS avg_emission_clear_days
FROM `ecofusion.weather_snapshot` w
JOIN `ecofusion.carbon_emissions` c ON LOWER(w.state) = LOWER(c.state)
GROUP BY w.state
ORDER BY avg_emission_clear_days;
```

More interesting queries are included in [`queries/analysis_queries.sql`](queries/analysis_queries.sql).

---
