# Consumer Spending & Economic Trends

## Overview
This project pulls data from multiple economic and financial APIs to analyze consumer spending trends based on inflation, income, and currency exchange rates.

## API Data Sources
1. **US Census Bureau API** (Household income data)
   - **URL:** `https://api.census.gov/data/2023/acs/acs5`
   - **Inputs:** State-level income data request (`B19013_001E`)
   - **Expected Output:**
     ```json
     [
       ["NAME", "B19013_001E", "state"],
       ["Alabama", "55000", "01"],
       ["California", "75000", "06"]
     ]
     ```

2. **FRED (Federal Reserve Economic Data)** (Inflation rates & interest rates)
   - **URL:** `https://api.stlouisfed.org/fred/series/observations`
   - **Inputs:** Series ID (e.g., CPIAUCSL for inflation)
   - **Expected Output:**
     ```json
     {
       "observations": [
         {"date": "2024-01-01", "value": "3.2"},
         {"date": "2024-02-01", "value": "3.1"}
       ]
     }
     ```

3. **Open Exchange Rates API** (Currency exchange data)
   - **URL:** `https://openexchangerates.org/api/latest.json`
   - **Inputs:** Base currency (USD)
   - **Expected Output:**
     ```json
     {
       "rates": {
         "EUR": 0.92,
         "GBP": 0.79,
         "JPY": 148.5
       },
       "timestamp": 1712083200
     }
     ```
