from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transform_to_csv import transform_carbon, transform_weather, transform_eia
from load_to_bigquery import load_csv_to_bigquery, tables
from get_data import main as ingest_main

def run_transform():
    transform_carbon()
    transform_weather()
    transform_eia()

def run_load():
    for table, filename in tables.items():
        load_csv_to_bigquery(table, filename)

# Ingest run locally - API Calls
# def run_ingest():        
#     ingest_main()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 28),
    'catchup': False
}

# Define the DAG
with DAG(
    dag_id='ecofusion_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger for now; we can automate later
    description='EcoFusion Sustainability Pipeline DAG'
) as dag:

    # Only two tasks now: transform and load
    transform_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=run_transform
    )

    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=run_load
    )

    transform_task >> load_task