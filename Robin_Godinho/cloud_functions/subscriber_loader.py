from google.cloud import pubsub_v1, bigquery
import json
import time
import os

project_id = "inst737-final-project"
subscription_id = "real-news-ingest-sub"

def callback(message):
    print("📩 Received Pub/Sub message.")
    payload = json.loads(message.data.decode("utf-8"))
    print(f"📦 Payload: {payload}")

    filename = payload["filename"]
    table_id = payload["table"]

    try:
        if not os.path.exists(filename):
            print(f"❌ File not found: {filename}")
            message.ack()
            return

        bq_client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True
        )

        with open(filename, "rb") as source_file:
            load_job = bq_client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )
        load_job.result()
        print(f"✅ Loaded {filename} into BigQuery table: {table_id}")
        message.ack()

    except Exception as e:
        print(f"❌ Error: {e}")
        message.nack()

subscriber = pubsub_v1.SubscriberClient()
sub_path = subscriber.subscription_path(project_id, subscription_id)

streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
print("🚀 Listening for Pub/Sub messages...")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    print("🛑 Stopping subscriber...")
    streaming_pull_future.cancel()
