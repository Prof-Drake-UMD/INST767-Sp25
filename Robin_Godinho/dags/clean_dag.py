from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from clean_data import clean_all

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('clean_news_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    clean_data_task = PythonOperator(
        task_id='clean_all_sources',
        python_callable=clean_all
    )
