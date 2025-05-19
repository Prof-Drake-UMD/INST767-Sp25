from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from PubSub.subscriber import listen_for_messages

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
}

with DAG(
    dag_id='subscribe_from_pubsub_pipeline',
    default_args=default_args,
    description='Listen to Pub/Sub and process messages into BigQuery',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["pubsub", "subscriber", "climate-data"]
) as dag:

    subscribe_task = PythonOperator(
        task_id='listen_for_pubsub_messages',
        python_callable=listen_for_messages
    )

    subscribe_task
