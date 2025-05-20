from google.cloud import bigquery

# 1) Initialize BigQuery client
client = bigquery.Client()

# 2) Set your destination table ID
#    Format: project_id.dataset_id.table_id
table_id = "inst767-459621.Location_Optimizer_Analytics.business"

# 3) Point to the CSV you wrote in GCS
gcs_uri = "gs://767-abinash/transform/yelp_businesses.csv"

# 4) Configure the load job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,     # skip the header row in your CSV
    autodetect=False,        # we provide the schema explicitly
    schema=[
        bigquery.SchemaField("name",         "STRING"),
        bigquery.SchemaField("rating",       "FLOAT"),
        bigquery.SchemaField("review_count", "INTEGER"),
        bigquery.SchemaField("categories",   "STRING"),
        bigquery.SchemaField("address",      "STRING"),
        bigquery.SchemaField("city",         "STRING"),
        bigquery.SchemaField("state",        "STRING"),
        bigquery.SchemaField("zip_code",     "STRING"),
        bigquery.SchemaField("latitude",     "FLOAT"),
        bigquery.SchemaField("longitude",    "FLOAT"),
        bigquery.SchemaField("phone",        "STRING"),
        bigquery.SchemaField("price",        "STRING"),
        bigquery.SchemaField("is_closed",    "BOOLEAN"),
    ],
    write_disposition="WRITE_APPEND"  # append to existing table
)

# 5) Kick off the load job
load_job = client.load_table_from_uri(
    gcs_uri,
    table_id,
    job_config=job_config
)

# 6) Wait for the job to finish
load_job.result()

# 7) Verify how many rows were loaded
table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows into {table_id}.")
