from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from ingest import fetch_newsdata, fetch_gnews, fetch_mediastack
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('news_ingest_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='fetch_newsdata',
        python_callable=fetch_newsdata,
        op_args=[os.getenv('NEWSDATA_API_KEY')]
    )

    t2 = PythonOperator(
        task_id='fetch_gnews',
        python_callable=fetch_gnews,
        op_args=[os.getenv('GNEWS_API_KEY')]
    )

    t3 = PythonOperator(
        task_id='fetch_mediastack',
        python_callable=fetch_mediastack,
        op_args=[os.getenv('MEDIASTACK_API_KEY')]
    )

    [t1, t2, t3]
