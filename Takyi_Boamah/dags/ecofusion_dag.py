# ecofusion_dag.py — Airflow DAG for EcoFusion Pipeline (Cloud Composer Ready)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from transform_to_csv import transform_carbon, transform_weather, transform_eia
from load_to_bigquery import load_csv_to_bigquery, tables, csv_folder

def run_transform():
    transform_carbon()
    transform_weather()
    transform_eia()

def run_load():
    for table in tables:
        filepath = os.path.join(csv_folder, tables[table])
        load_csv_to_bigquery(table, filepath)

def run_ingest():
    from get_data import main as ingest_main
    ingest_main()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

# Define the DAG
with DAG(
    dag_id='ecofusion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    description='Orchestrates EcoFusion data ingestion, transformation, and load to BigQuery'
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_api_data',
        python_callable=run_ingest
    )

    transform_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=run_transform
    )

    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=run_load
    )

    ingest_task >> transform_task >> load_task
