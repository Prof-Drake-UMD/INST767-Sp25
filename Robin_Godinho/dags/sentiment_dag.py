from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sentiment_analysis import analyze_sentiment

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('sentiment_analysis_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    sentiment_task = PythonOperator(
        task_id='run_sentiment_analysis',
        python_callable=analyze_sentiment
    )
