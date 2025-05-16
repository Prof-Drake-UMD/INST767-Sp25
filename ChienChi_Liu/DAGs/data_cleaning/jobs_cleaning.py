import os, json, pandas as pd

def transform_job_data(input_file='data', output_file='transformed_data'):
    df_adzuna = pd.read_json(f'../{input_file}/adzuna_jobs.json')
    df_jooble = pd.read_json(f'../{input_file}/jooble_jobs.json')
    df_muse = pd.read_json(f'../{input_file}/muse_jobs.json')

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
    combined_df.to_json(f'../{output_file}/jobs_data_standardized.json', orient='records', indent=4)

    return combined_df

def main():
    input_file = 'data'
    output_file = 'transformed_data'
    transform_job_data(input_file, output_file)
if __name__ == "__main__":
    main()