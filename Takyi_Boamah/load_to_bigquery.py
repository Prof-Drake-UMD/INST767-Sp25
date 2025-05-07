from google.cloud import bigquery

project_id = "ecofusion-pipeline"    
dataset_id = "ecofusion"
bucket_name = "us-central1-ecofusion-airfl-159dd45e-bucket"
csv_folder_gcs = f"gs://{bucket_name}/data"

tables = {
    "carbon_emissions": "carbon_emissions.csv",
    "weather_snapshot": "weather_snapshot.csv",
    "state_electricity_emissions": "state_electricity_emissions.csv"
}

def load_csv_to_bigquery(table_name, file_name):
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    gcs_uri = f"{csv_folder_gcs}/{file_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )
    load_job.result()  # Wait for the job to complete
    print(f"✅ Loaded {table_name} from {gcs_uri} into {table_id}")
