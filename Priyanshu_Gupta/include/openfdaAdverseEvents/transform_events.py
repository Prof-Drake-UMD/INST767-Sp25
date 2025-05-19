import logging
import pandas as pd
import json
from io import BytesIO
import os
# from google.cloud import storage, bigquery

import yaml
# import pyarrow
# import pyarrow.parquet as pq


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Load YAML
BASE_DIR = os.path.dirname(__file__)
yaml_path = os.path.join(BASE_DIR, "code_maps.yml")

with open(yaml_path, "r") as f:
    config = yaml.safe_load(f)


def process_event_info(df):
    required_columns = [
        'safetyreportid', 
        'occurcountry', 
        'receivedate', 
        'serious',
        'primarysource.reportercountry',
        'primarysource.qualification',
        'seriousnessdeath',
        'seriousnesslifethreatening', 
        'seriousnesshospitalization',
        'seriousnessdisabling', 
        'seriousnesscongenitalanomali',
        'patient.patientonsetage',
        'patient.patientagegroup',
        'patient.patientsex',
        'patient.patientweight',
        'patient.reaction',
        'patient.drug'
    ]

    qualification_map = config['code_maps']['qualification']

    # Map to DataFrame column
    df['primarysource.qualification'] = df['primarysource.qualification'].map(qualification_map)


    patientsex_map = config['code_maps']['patientsex']

    # Map to DataFrame column
    df['patient.patientsex'] = df['patient.patientsex'].map(patientsex_map)

    agegroup_map = config['code_maps']['agegroup']

    # Map to DataFrame column
    df['patient.patientagegroup'] = df['patient.patientagegroup'].map(agegroup_map)



    return df[required_columns].reset_index(drop=True)

     
def process_reaction_info(df, reaction_col):

    reactions = df[['safetyreportid', reaction_col]]

    reactions_exploded = reactions.explode(reaction_col)

    reactions_df = pd.concat([
        reactions_exploded[['safetyreportid']].reset_index(drop=True),
        pd.json_normalize(reactions_exploded[reaction_col])
    ], axis=1
    )

    # Access a specific map
    reactionoutcome_map = config['code_maps']['reactionoutcome']

    # Map to DataFrame column
    reactions_df['reactionoutcomedesc'] = reactions_df['reactionoutcome'].map(reactionoutcome_map)


    reaction_cols = [
                    # 'safetyreportid',
                    'reactionmeddrapt',
                    'reactionoutcome',
                    'reactionoutcomedesc'
                    ]
    
    
    grouped_df = (
    reactions_df.groupby('safetyreportid')[reaction_cols]
    .apply(lambda x: x.to_dict(orient='records')) 
    .reset_index(name='reaction') 
    )

    return grouped_df
    


def process_drug_identifier(df, drug_column): 


    drugs_identifier = df[['safetyreportid', drug_column]]

    drugs_exploded = drugs_identifier.explode(drug_column)


    drug_df = pd.concat([
        drugs_exploded[['safetyreportid']].reset_index(drop=True),
        pd.json_normalize(drugs_exploded[drug_column])
    ], axis=1
    )

    #taking only one NDC code
    drug_df['product_ndc'] = drug_df['openfda.product_ndc'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
    
    drugcharaterization_map = config['code_maps']['drugcharacterization']

    # Map to DataFrame column
    drug_df['drugcharacterizationdesc'] = drug_df['drugcharacterization'].map(drugcharaterization_map)

    drug_df.rename(columns={'activesubstance.activesubstancename': 'activesubstancename'}, inplace=True)

    drug_info_cols = [
                    # 'safetyreportid',
                    'drugcharacterization',
                    'drugcharacterizationdesc',
                    'medicinalproduct', 
                    'product_ndc',
                    'drugindication', 
                    'drugdosageform',
                    'activesubstancename'
    ]

    
    grouped_df = (
    drug_df.groupby('safetyreportid')[drug_info_cols]
    .apply(lambda x: x.to_dict(orient='records')) 
    .reset_index(name='drugsinfo') 
    )

    return grouped_df

def join_df(events_df, reaction_df, drug_df):
     # Step 1: Merge events_df with reaction_df on 'safetyreportid'

    events_df=events_df.drop(columns=['patient.reaction', 'patient.drug'])
    combined_df = pd.merge(events_df, reaction_df, on='safetyreportid', how='left')

    # Step 2: Merge the result with drug_df on 'safetyreportid'
    combined_df = pd.merge(combined_df, drug_df, on='safetyreportid', how='left')

    return combined_df

def rename_columns(df):
    df.columns = [col.split('.')[1].lower() if '.' in col else col for col in df.columns]
    df.rename(columns={'qualification': 'reporterqualification'}, inplace=True)
    df['receivedate'] = pd.to_datetime(df['receivedate'], format='%Y%m%d').dt.strftime('%Y-%m-%d')


    return df



def transform_drug_events(raw_df): 

    events_df=process_event_info(raw_df)
    reaction_df=process_reaction_info(events_df, 'patient.reaction')
    drug_df=process_drug_identifier(events_df, 'patient.drug')

    combined_df = join_df(events_df, reaction_df, drug_df)
    combined_df = rename_columns(combined_df)

    combined_df = combined_df.where(pd.notnull(combined_df), None)

    # Convert DataFrame to a list of dictionaries (records)
    json_data = combined_df.to_json(orient="records", lines=True)

    return json_data



if __name__ == '__main__':
    transform_drug_events()




    



   