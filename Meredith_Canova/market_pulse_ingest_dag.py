import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ── Trigger Cloud Function ─────────────────────────────────────
def trigger_http_function(url: str):
    response = requests.post(url)
    response.raise_for_status()

# ── Default DAG Settings ───────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='market_pulse_ingest_dag',
    default_args=default_args,
    description='Trigger Market Pulse ingest functions on weekdays only',
    schedule_interval='10 21 * * 1-5',  
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=['market', 'etl'],
)

# ── Ingest Tasks ───────────────────────────────────────────────
ingest_stocks = PythonOperator(
    task_id='ingest_stocks',
    python_callable=trigger_http_function,
    op_args=["https://ingest-stocks-j5sbqsxjta-uc.a.run.app"],
    dag=dag,
)

ingest_news = PythonOperator(
    task_id='ingest_news',
    python_callable=trigger_http_function,
    op_args=["https://ingest-news-j5sbqsxjta-uc.a.run.app"],
    dag=dag,
)

ingest_trends = PythonOperator(
    task_id='ingest_trends',
    python_callable=trigger_http_function,
    op_args=["https://ingest-trends-j5sbqsxjta-uc.a.run.app"],
    dag=dag,
)


