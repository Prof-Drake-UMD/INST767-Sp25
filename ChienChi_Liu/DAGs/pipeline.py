import os
import json
import pandas as pd
from api_connection.adzuna_api import AdzunaConnector
from api_connection.jooble_api import JoobleConnector
from api_connection.muse_api import MuseConnector

# Extraction
def extract_data():
    os.makedirs('data', exist_ok=True)

    muse_api_key = os.environ.get('MUSE_API_KEY')
    muse_connector = MuseConnector(muse_api_key)
    muse_jobs = muse_connector.extract_jobs(categories=["ux","design","management","ui","product","interaction","engineer"], page_count=1, job_count_per_page=5)
    with open("data/muse_jobs.json", "w") as f:
            f.write(json.dumps(muse_jobs, indent=2))

    adzuna_api_id = os.environ.get('ADZUNA_APP_ID')
    adzuna_api_key = os.environ.get('ADZUNA_APP_KEY')
    adzuna_connector = AdzunaConnector(adzuna_api_id, adzuna_api_key)
    adzuna_jobs = adzuna_connector.extract_jobs(keywords=["software","data","devops","engineer","IT","developer","designer","manager"])
    with open("data/adzuna_jobs.json", "w") as f:
            f.write(json.dumps(adzuna_jobs, indent=2)) 

    jooble_api_key = os.environ.get('JOOBLE_API_KEY')
    jooble_connector = JoobleConnector(jooble_api_key)
    jooble_jobs = jooble_connector.extract_jobs(keywords=["engineer","designer"], locations=["remote"], limit=20)
    with open("data/jooble_jobs.json", "w") as f:
            f.write(json.dumps(jooble_jobs, indent=4))

# Transformation
def transform_data():
    os.makedirs('transformed_data', exist_ok=True)

    df_adzuna = pd.read_json('data/adzuna_jobs.json')
    df_jooble = pd.read_json('data/jooble_jobs.json')
    df_muse = pd.read_json('data/muse_jobs.json')

    field_mappings = {
        'df_adzuna': {
            'job_title': 'title',
            'job_description': 'description',
            'job_url': 'redirect_url',
            'posted_date': 'created',
            'job_category': 'category.label',
            'job_type': 'contract_time',
            'company_name': 'company.display_name',
            'salary': 'combined_salary_string',   
            'salary_min': 'salary_min',
            'salary_max': 'salary_max',
        },
        'df_jooble': {
            'job_title': 'title',
            'job_description': 'snippet',
            'job_url': 'link',
            'posted_date': 'updated',
            'job_category': 'type',
            'job_type': 'type',
            'company_name': 'company',
            'salary': 'salary',
        },
        'df_muse': {
            'job_title': 'name',
            'job_description': 'contents',
            'job_url': 'refs.landing_page',
            'posted_date': 'publication_date',
            'job_category': 'categories[0].name', 
            'job_type': '',   
            'company_name': 'company.name',
            'salary': ''
        }
    }

    mapping_adzuna = field_mappings['df_adzuna']
    mapping_jooble = field_mappings['df_jooble']
    mapping_muse = field_mappings['df_muse']
    df_standardized_adzuna = pd.DataFrame()
    df_standardized_jooble = pd.DataFrame()
    df_standardized_muse = pd.DataFrame()

    for new_col, original_col in mapping_adzuna.items():
        if new_col not in ['salary_min', 'salary_max']:
            if '.' in original_col:
                parts = original_col.split('.')
                if parts[0] in df_adzuna.columns:
                    df_standardized_adzuna[new_col] = df_adzuna[parts[0]].apply(
                        lambda x: x.get(parts[1]) if isinstance(x, dict) and parts[1] in x else None
                    )
            else:
                if original_col in df_adzuna.columns:
                    df_standardized_adzuna[new_col] = df_adzuna[original_col]
                else:
                    df_standardized_adzuna[new_col] = None
    # Handle salary separately
    if 'salary_min' in mapping_adzuna and 'salary_max' in mapping_adzuna:
        min_col = mapping_adzuna['salary_min']
        max_col = mapping_adzuna['salary_max']

        if min_col in df_adzuna.columns and max_col in df_adzuna.columns:
            df_standardized_adzuna['salary'] = '$' + df_adzuna[min_col].astype(str) + ' - ' + '$' + df_adzuna[max_col].astype(str)
            df_standardized_adzuna['salary'] = df_standardized_adzuna['salary'].str.replace('nan', '', regex=False)
        else:
            df_standardized_adzuna['salary'] = None

    for new_col, original_col in mapping_jooble.items():
        if original_col in df_jooble.columns:
            df_standardized_jooble[new_col] = df_jooble[original_col]
        else:
            df_standardized_jooble[new_col] = None

    for new_col, original_col in mapping_muse.items():
        if '.' in original_col:
            parts = original_col.split('.')
            if parts[0] in df_muse.columns:
                df_standardized_muse[new_col] = df_muse[parts[0]].apply(
                    lambda x: x.get(parts[1]) if isinstance(x, dict) and parts[1] in x else None
                )
            elif parts[0] == 'categories[0]' and 'categories' in df_muse.columns:
                df_standardized_muse[new_col] = df_muse['categories'].apply(
                    lambda x: x[0].get('name') if isinstance(x, list) and len(x) > 0 and 'name' in x[0] else None
                )
        else:
            if original_col in df_muse.columns:
                df_standardized_muse[new_col] = df_muse[original_col]
            else:
                df_standardized_muse[new_col] = None

    combined_df = pd.concat([df_standardized_adzuna, df_standardized_jooble, df_standardized_muse], ignore_index=True)
    combined_df.to_json('transformed_data/jobs_data_standardized.json', orient='records', indent=4)
    combined_df.to_csv('transformed_data/jobs_data_standardized.csv', index=False)

if __name__ == "__main__":
    extract_data()
    transform_data()
    print("Pipeline executed successfully.")