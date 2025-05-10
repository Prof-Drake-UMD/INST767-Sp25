from flask import Flask, request
import pandas as pd
import os
import base64
from google.cloud import storage

app = Flask(__name__)

@app.route('/', methods=['GET'])
def health_check():
    return "Transform service is up!"

@app.route('/', methods=['POST'])
def transform_pipeline():
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        return ('Bad Request: Invalid Pub/Sub message', 400)

    pubsub_message = envelope['message']

    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        message_data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        print(f"Received Pub/Sub message: {message_data}")
        transform_data()

    return ('', 204)

def transform_data():
    from google.cloud import storage, bigquery
    import pandas as pd
    from datetime import datetime

    print("Starting data transformation and upload to BigQuery")

    # Setup
    bucket_name = "jennifer-finanical-bucket"
    csv_files = {
        "aapl_stock_data.csv": {
            "table_id": "inst767final.aapl_stock_data",
            "schema": [
                bigquery.SchemaField("Date", "DATE"),
                bigquery.SchemaField("Open", "FLOAT"),
                bigquery.SchemaField("High", "FLOAT"),
                bigquery.SchemaField("Low", "FLOAT"),
                bigquery.SchemaField("Close", "FLOAT"),
                bigquery.SchemaField("Volume", "INTEGER"),
                bigquery.SchemaField("Dividends", "FLOAT"),
                bigquery.SchemaField("Stock Splits", "FLOAT"),
            ],
        },
        "bitcoin_data.csv": {
            "table_id": "inst767final.bitcoin_data",
            "schema": [
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("price_usd", "FLOAT"),
            ],
        },
        "macro_indicators.csv": {
            "table_id": "inst767final.macro_indicators",
            "schema": [
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("value", "FLOAT"),
            ],
        },
    }

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    for filename, config in csv_files.items():
        print(f"Processing {filename}...")

        # Download file from GCS
        blob = storage_client.bucket(bucket_name).blob(filename)
        local_path = f"/tmp/{filename}"
        blob.download_to_filename(local_path)

        # Load and clean data
        df = pd.read_csv(local_path)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Save cleaned version
        cleaned_path = f"/tmp/cleaned_{filename}"
        df.to_csv(cleaned_path, index=False)

        # Upload to BigQuery
        job_config = bigquery.LoadJobConfig(
            schema=config["schema"],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition="WRITE_TRUNCATE",
        )

        with open(cleaned_path, "rb") as source_file:
            job = bq_client.load_table_from_file(
                source_file,
                config["table_id"],
                job_config=job_config
            )
        job.result()
        print(f"Uploaded {filename} to BigQuery table {config['table_id']}")

    print("All datasets processed and uploaded successfully!")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
