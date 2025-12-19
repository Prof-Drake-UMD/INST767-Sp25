
# Macro Economic Indicators Project

## Overview
This project demonstrates how macroeconomic datasets were ingested, transformed, and joined using Google BigQuery.
The final output is a single joined table combining inflation, employment, and unemployment indicators.

## Data Sources
The project uses three datasets obtained from the U.S. Bureau of Labor Statistics (BLS):

1. **CPI Inflation**
   - Table name: `cpi_inflation`
   - Fields: year, period, month, cpi_value

2. **CES Employment**
   - Table name: `ces_employment`
   - Fields: industry, year, period, month, employment_level

3. **LAUS Unemployment**
   - Table name: `laus_unemployment`
   - Fields: year, period, month, unemployment_rate

## Data Ingestion
- Python was used only to upload the raw datasets into Google BigQuery.
- No transformations or cleaning were performed in Python.

## Data Transformation (BigQuery)
All transformations were performed directly in **Google BigQuery using SQL**.

Steps:
- Joined the three tables on `year` and `period`
- Preserved all relevant macroeconomic indicators
- Created a final table called `joined_table`

## SQL Join Logic
The join aligns inflation, employment, and unemployment metrics by time period:
- CPI ↔ Employment on (year, period)
- CPI ↔ Unemployment on (year, period)

## Final Output
- Dataset: `inst767_sp25`
- Final table: `joined_table`

## Files Included in Repository
- SQL file containing the join query
- README.txt

## Tools Used
- Google BigQuery
- BigQuery SQL
- Python (data ingestion only)
- GitHub

## Notes
- No aggregations or visualizations were required.
- All analytical logic resides in BigQuery SQL.
