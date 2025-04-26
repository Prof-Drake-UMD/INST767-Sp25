from google.cloud import bigquery
import os

# Set up environment
project_id = "ecofusion-pipeline"  # Replace with your GCP project ID
dataset_id = "ecofusion"            # Your BigQuery dataset name
csv_folder = "./transformed"         # Folder where CSVs were output

# Table-to-CSV mapping
tables = {
    "carbon_emissions": "carbon_emissions.csv",
    "weather_snapshot": "weather_snapshot.csv",
    "state_electricity_emissions": "state_electricity_emissions.csv"
}

# Authenticate using environment variable or service account
# export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account.json"

client = bigquery.Client(project=project_id)

for table_name, file_name in tables.items():
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    file_path = os.path.join(csv_folder, file_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Wait for job to finish
        print(f"✅ Loaded {table_name} into {table_id}")
