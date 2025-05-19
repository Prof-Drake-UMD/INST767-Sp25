from google.cloud import bigquery



def load_to_bq(transformed_df, table_name, merge_condition):

  project_id = 'inst767-openfda-pg'

  staging_table = f'{project_id}.staging.{table_name}'
  target_table = f'{project_id}.openfda.{table_name}'


  bqClient = bigquery.Client()
  # Upload to staging
  job = bqClient.load_table_from_dataframe(
  transformed_df,
  staging_table,
  job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
  )

  print(f"Loaded into big query staging {staging_table}")

  # MERGE into target table
  merge_query = f"""
  MERGE `{target_table}` target
  USING `{staging_table}` staging
  ON {merge_condition}
  WHEN NOT MATCHED THEN
    INSERT ROW
  """
  bqClient.query(merge_query).result()
