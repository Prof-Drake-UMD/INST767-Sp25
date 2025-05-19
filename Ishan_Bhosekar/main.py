from Ishan_Bhosekar.dag.transform.transform_data import transform_weather_data, transform_air_quality_data, transform_gdp_data
from Ishan_Bhosekar.dag.ingest.api_calls import fetch_weather_data, fetch_air_quality_data, fetch_worldbank_data
from Ishan_Bhosekar.dag.load.bigquery_loader import load_json_to_bq
import pandas as pd

def main():
    weather = fetch_weather_data("New York", "bf32905a0da7fece2559e5c367e5c234")
    air = fetch_air_quality_data("New York", "2158bce9b080ddecbd37aaf7bdb68ac88e2cbe78")
    gdp = fetch_worldbank_data("US", "NY.GDP.MKTP.CD")

    print("✅ Weather Data:", weather)
    print("✅ Air Quality Data:", air)
    print("✅ GDP Data (Top 3):", gdp[:3])

    # 🚀 Apply transformations
    weather_transformed = transform_weather_data([weather])
    air_transformed = transform_air_quality_data([air])
    gdp_transformed = transform_gdp_data(gdp)

    # 🚀 Load transformed data into BigQuery
    load_json_to_bq("climate_data", "weather", weather_transformed)
    load_json_to_bq("climate_data", "air_quality", air_transformed)
    load_json_to_bq("climate_data", "gdp", gdp_transformed)

    # 🚀 Save to CSV locally
    pd.DataFrame(weather_transformed).to_csv("weather_transformed.csv", index=False)
    pd.DataFrame(air_transformed).to_csv("air_quality_transformed.csv", index=False)
    pd.DataFrame(gdp_transformed).to_csv("gdp_transformed.csv", index=False)

if __name__ == "__main__":
    main()
