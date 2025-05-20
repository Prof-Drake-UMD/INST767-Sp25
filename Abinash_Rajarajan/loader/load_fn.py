import os, json, base64, functions_framework
from google.cloud import bigquery

@functions_framework.cloud_event
def load_fn(cloud_event):
    project = os.environ["GCP_PROJECT"]
    dataset = "Location_Optimizer_Analytics"

    data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]))
    bucket, path, domain = data["bucket"], data["path"], data["domain"]

    client = bigquery.Client(project=project)
    table  = f"{project}.{dataset}.{domain}"
    uri    = f"gs://{bucket}/{path}"

    schemas = {
      "events":[ 
         bigquery.SchemaField("name","STRING"),
         bigquery.SchemaField("postalCode","STRING"),
         bigquery.SchemaField("startDateTime","TIMESTAMP"),
         bigquery.SchemaField("endDateTime","TIMESTAMP"),
      ],
      "weather":[ 
         bigquery.SchemaField("city","STRING"),
         bigquery.SchemaField("country","STRING"),
         bigquery.SchemaField("latitude","FLOAT"),
         bigquery.SchemaField("longitude","FLOAT"),
         bigquery.SchemaField("weather_main","STRING"),
         bigquery.SchemaField("weather_desc","STRING"),
         bigquery.SchemaField("temperature","FLOAT"),
         bigquery.SchemaField("humidity","INTEGER"),
         bigquery.SchemaField("pressure","INTEGER"),
         bigquery.SchemaField("wind_speed","FLOAT"),
         bigquery.SchemaField("timestamp","TIMESTAMP"),
      ],
      "business":[
         bigquery.SchemaField("name","STRING"),
         bigquery.SchemaField("rating","FLOAT"),
         bigquery.SchemaField("review_count","INTEGER"),
         bigquery.SchemaField("categories","STRING"),
         bigquery.SchemaField("address","STRING"),
         bigquery.SchemaField("city","STRING"),
         bigquery.SchemaField("state","STRING"),
         bigquery.SchemaField("zip_code","STRING"),
         bigquery.SchemaField("latitude","FLOAT"),
         bigquery.SchemaField("longitude","FLOAT"),
         bigquery.SchemaField("price","STRING"),
         bigquery.SchemaField("is_closed","BOOLEAN"),
      ]
    }

    job = client.load_table_from_uri(uri, table,
      job_config=bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.CSV,
        skip_leading_rows = 1,
        schema            = schemas[domain],
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
      ))
    job.result()
