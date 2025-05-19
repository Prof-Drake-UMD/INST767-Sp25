
import base64
from io import BytesIO
import logging
import json
from google.cloud import bigquery
from transform import transform_ndc_directory, transform_recall_enforcements
from read_from_uri import read_from_uri
from load_to_bq import load_to_bq
import pandas as pd



def main(event, context):
    message = base64.b64decode(event['data']).decode('utf-8')
    message_dict = json.loads(message)

    logging.info(f"Received message: {message_dict}")

    table_name = message_dict['table_name']
    gcs_path = message_dict["storage_path"]

    logging.info(f"Processing table: {table_name}, from: {gcs_path}")

    df = read_from_uri(gcs_path) 

    # print(df.dtypes)

    if table_name == 'ndc-directory':
       transformed_df = transform_ndc_directory(df)
       merge_condition = "target.product_ndc = staging.product_ndc"
    

    elif table_name == 'recall-enforcements':
        transformed_df = transform_recall_enforcements(df)
        merge_condition ="target.product_ndc = staging.product_ndc AND target.recall_initiation_date = staging.recall_initiation_date"

    load_to_bq(transformed_df, table_name, merge_condition)


