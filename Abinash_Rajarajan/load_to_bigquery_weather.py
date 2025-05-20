from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define table ID (project.dataset.table)
table_id = "inst767-459621.Location_Optimizer_Analytics.weather"

# Define GCS file path
gcs_uri = "gs://767-abinash/transform/weather-openweather.csv"

# Configure the load job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Skip header row
    autodetect=False,      # Disable schema autodetection
    schema=[
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("weather_main", "STRING"),
        bigquery.SchemaField("weather_description", "STRING"),
        bigquery.SchemaField("temperature", "FLOAT"),
        bigquery.SchemaField("humidity", "INTEGER"),
        bigquery.SchemaField("pressure", "INTEGER"),
        bigquery.SchemaField("wind_speed", "FLOAT"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
    ]
)

# Load data from GCS to BigQuery
load_job = client.load_table_from_uri(
    gcs_uri, table_id, job_config=job_config
)

# Wait for the job to complete
load_job.result()

# Print the result
table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows into {table_id}.")
