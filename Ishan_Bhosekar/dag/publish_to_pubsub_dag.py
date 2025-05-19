from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from PubSub.publisher import publish_messages  # your function that publishes to Pub/Sub

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
}

with DAG(
    dag_id='publish_to_pubsub_dag',
    default_args=default_args,
    description='Publish API data messages to Pub/Sub',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["pubsub", "publisher"],
) as dag:

    publish_task = PythonOperator(
        task_id='publish_messages_task',
        python_callable=publish_messages
    )
