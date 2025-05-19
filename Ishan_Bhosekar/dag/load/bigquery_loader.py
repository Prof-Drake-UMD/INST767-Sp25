from google.cloud import bigquery

def load_json_to_bq(dataset_name, table_name, rows_to_insert):
    project_id = "climate-data-pipeline-457720"   # ✅ Real project ID
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"❌ Encountered errors while inserting rows: {errors}")
    else:
        print(f"✅ Successfully inserted into {table_id}")

