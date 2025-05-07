from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from get_data import main as ingest_main
from transform_to_csv import transform_carbon, transform_weather, transform_eia
from load_to_bigquery import load_csv_to_bigquery, tables

# DAG callable wrappers
def run_ingest():
    ingest_main()

def run_transform():
    transform_carbon()
    transform_weather()
    transform_eia()

def run_load():
    for table, filename in tables.items():
        load_csv_to_bigquery(table, filename)

# Default DAG config
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 28),
    'catchup': False
}

with DAG(
    dag_id='ecofusion_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Or use '@daily' later
    description='EcoFusion Sustainability Pipeline'
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

    # Define task order
    ingest_task >> transform_task >> load_task
