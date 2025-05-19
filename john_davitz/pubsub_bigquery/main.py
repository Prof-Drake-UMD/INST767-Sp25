import base64
import json
import pandas as pd
import io
from google.cloud import storage
import os
from google.cloud import pubsub_v1
from google.cloud import bigquery

def fetch_and_upload(event, context):
    print("signal recieved - running")
    
    print("Starting Metro")
    metro_csv = "gs://api_output_bucket_inst_final/combined_output/combined_metro.csv"
    
    df = pd.read_csv(metro_csv)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    client = bigquery.Client()
    table_id = "instfinal-459621.inst_final.metro"

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    if job.errors:
        print("Metro Errors:", job.errors)
    else:
        print("Metro upload completed successfully.")

    #========================== Weather
    weather_csv = "gs://api_output_bucket_inst_final/combined_output/final_weather.csv"
    
    df = pd.read_csv(weather_csv)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    client = bigquery.Client()
    table_id = "instfinal-459621.inst_final.weather"

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    if job.errors:
        print("Weather Errors:", job.errors)
    else:
        print("Weather upload completed successfully.")

    #========================== Traffic
    traffic_df = "gs://api_output_bucket_inst_final/combined_output/final_traffic.csv"
    
    df = pd.read_csv(traffic_df)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    client = bigquery.Client()
    table_id = "instfinal-459621.inst_final.traffic"

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    if job.errors:
        print("Traffic Errors:", job.errors)
    else:
        print("Traffic upload completed successfully.")