

from urllib.parse import urlparse
import pandas as pd


def transform_ndc_directory(raw_df):

    req_fields = [
    "product_ndc",
    "generic_name",
    "brand_name",
    "labeler_name",
    "dosage_form",
    "route",
    "marketing_category",
    "marketing_start_date"
    ]

    filter_df = raw_df[req_fields]

    filter_df['route'] = filter_df['route'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)
    filter_df['marketing_start_date'] = pd.to_datetime(filter_df['marketing_start_date'], format='%Y%m%d')


    return filter_df



def transform_recall_enforcements(raw_df):
    req_fields = [
        'openfda.product_ndc',
        'classification',
        'status',
        'state',
        'country',
        'recalling_firm',
        'reason_for_recall',
        'recall_initiation_date',
        'termination_date'
    ]

    filter_df = raw_df[req_fields]

    #taking only one NDC code
    filter_df['product_ndc'] = filter_df['openfda.product_ndc'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
    filter_df = filter_df[['product_ndc'] + [col for col in filter_df.columns if col != 'product_ndc']]
    filter_df = filter_df.drop('openfda.product_ndc', axis=1)
    
    #convert date columns
    filter_df["recall_initiation_date"] = pd.to_datetime(filter_df['recall_initiation_date'], format='%Y%m%d')
    filter_df["termination_date"] = pd.to_datetime(filter_df['termination_date'], format='%Y%m%d')


    return filter_df
















