from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

from ingest.api_calls import fetch_weather_data, fetch_air_quality_data, fetch_worldbank_data
from load.bigquery_loader import load_json_to_bq
from transform.transform_data import transform_weather_data, transform_air_quality_data, transform_gdp_data


# List of cities and countries
CITIES = [
    "New York", "Toronto", "London", "Beijing", "Tokyo", "Berlin", "Paris", "Mumbai",
    "São Paulo", "Moscow", "Sydney", "Rome", "Mexico City", "Seoul", "Johannesburg",
    "Jakarta", "Riyadh", "Istanbul", "Madrid", "Lagos"
]

COUNTRIES = [
    "US", "CA", "GB", "CN", "JP", "DE", "FR", "IN", "BR", "RU",
    "AU", "IT", "MX", "KR", "ZA", "ID", "SA", "TR", "ES", "NG"
]

# Default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
}

# Tasks
def extract_weather():
    OPENWEATHER_API_KEY = Variable.get("OPENWEATHER_API_KEY")
    weather_data = []
    for city in CITIES:
        weather = fetch_weather_data(city, OPENWEATHER_API_KEY)
        weather_data.append(weather)

    # 🚀 Transform
    transformed_weather = transform_weather_data(weather_data)
    load_json_to_bq("climate_data", "weather", transformed_weather)


def extract_air_quality():
    WAQI_TOKEN = Variable.get("WAQI_TOKEN")
    air_data = []
    for city in CITIES:
        air = fetch_air_quality_data(city, WAQI_TOKEN)
        air_data.append(air)

    # 🚀 Transform
    transformed_air = transform_air_quality_data(air_data)
    load_json_to_bq("climate_data", "air_quality", transformed_air)


def extract_gdp():
    gdp_data = []
    for country in COUNTRIES:
        gdp = fetch_worldbank_data(country, "NY.GDP.MKTP.CD")
        gdp_data.extend(gdp)  # because World Bank returns a list

    # 🚀 Transform
    transformed_gdp = transform_gdp_data(gdp_data)
    load_json_to_bq("climate_data", "gdp", transformed_gdp)


# Define the DAG
with DAG(
        dag_id='climate_data_pipeline',
        default_args=default_args,
        description='Pipeline to pull Weather, Air Quality, and GDP data daily',
        schedule_interval='0 */6 * * *',  # Every 6 hours
        catchup=False,
        tags=["climate", "economic", "data_pipeline"]
) as dag:
    weather_task = PythonOperator(
        task_id='extract_and_load_weather',
        python_callable=extract_weather
    )

    air_quality_task = PythonOperator(
        task_id='extract_and_load_air_quality',
        python_callable=extract_air_quality
    )

    gdp_task = PythonOperator(
        task_id='extract_and_load_gdp',
        python_callable=extract_gdp
    )

    weather_task >> air_quality_task >> gdp_task

