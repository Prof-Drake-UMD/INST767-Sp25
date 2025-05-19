import os
import pandas as pd
from datetime import datetime
from ingest import fetch_dc_data, fetch_nyc_data, fetch_cdc_data
from transform import transform_dc_data, transform_nyc_data, transform_cdc_data, combine_all
from google.cloud import storage, bigquery

# Output directory for processed CSVs
OUTPUT_DIR = "data/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_local_pipeline():
    print("Starting local ETL pipeline...")

    # Step 1:Ingesting data
    dc_json = fetch_dc_data()
    nyc_json = fetch_nyc_data()
    cdc_json = fetch_cdc_data()

    print("\nIngest Complete")

    # Step 2:Transforming each dataset (print statements to log actions)
    print("\nTransforming DC data...")
    dc_df = transform_dc_data(dc_json)
    print("DC rows:", len(dc_df))
    print(dc_df[["incident_date", "description", "location", "latitude", "longitude"]].head(2))

    print("\nTransforming NYC data...")
    nyc_df = transform_nyc_data(nyc_json)
    print("NYC rows:", len(nyc_df))
    print(nyc_df[["incident_date", "description", "location", "latitude", "longitude"]].head(2))

    print("\nTransforming CDC data...")
    cdc_df = transform_cdc_data(cdc_json)
    print("CDC rows:", len(cdc_df))
    print(cdc_df[["incident_date", "description", "location"]].head(2))

    # Step 3:Combining all datasets into a unified one
    print("\nCombining data...")
    final_df = combine_all(dc_df, nyc_df, cdc_df)

    print("\nFinal Combined DataFrame:")
    print(final_df.head(5))
    print("Columns:", final_df.columns.tolist())
    print("Total rows:", len(final_df))
    print("Data types:\n", final_df.dtypes)

    # Step 4:Saving results to csv file
    output_path = os.path.join(OUTPUT_DIR, "firearm_incidents_file.csv")
    final_df.to_csv(output_path, index=False)
    print(f"\nETL pipeline complete. Output saved to {output_path}")

    #uploading to gcs
    bucket_name = "gv-etl-bucket"
    destination_blob = "firearm_incidents_file.csv"  
    upload_to_gcs(bucket_name, output_path, destination_blob)
    
    #loading to BQ
    gcs_uri = f"gs://{bucket_name}/{destination_blob}"
    table_id = "gv-etl-spring.gun_violence_dataset.firearm_incidents"
    load_csv_to_bigquery(gcs_uri, table_id)


#uploading file to cloud storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name): 
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"Uploaded to GCS: gs://{bucket_name}/{destination_blob_name}")


#loading csv from gcs to BigQuery table (overwrites everytime)
def load_csv_to_bigquery(uri, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE" #overwrites table
    )
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    print(f"Loaded data into BigQuery: {table_id}")



if __name__ == "__main__":
    run_local_pipeline()
