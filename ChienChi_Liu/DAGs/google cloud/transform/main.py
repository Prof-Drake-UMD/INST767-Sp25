import os
import json
import base64
import flask
import pandas as pd
from google.cloud import storage

PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = f"job-data-{PROJECT_ID}"

app = flask.Flask(__name__)

def download_json_from_gcs(bucket_name, source_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        
        if not blob.exists():
            print(f"File {source_blob_name} not found in bucket {bucket_name}")
            return pd.DataFrame()
            
        json_content = blob.download_as_text()
        data = json.loads(json_content)
        df = pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame([data])
        print(f"Successfully downloaded and parsed {source_blob_name}")
        return df
    
    except Exception as e:
        print(f"Error downloading {source_blob_name}: {str(e)}")
        return pd.DataFrame()

def upload_to_gcs(data, destination_blob_name, bucket_name=BUCKET_NAME):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        if isinstance(data, pd.DataFrame):
            json_data = data.to_json(orient='records', indent=4)
            blob.upload_from_string(json_data, content_type="application/json")
        else:
            blob.upload_from_string(data, content_type="application/json")

        print(f"File {destination_blob_name} uploaded to {bucket_name}")
        
        if blob.exists():
            print(f"Verified upload of {destination_blob_name}")
            return True
        else:
            print(f"Upload verification failed for {destination_blob_name}")
            return False
    except Exception as e:
        print(f"Error uploading to GCS: {str(e)}")
        return False

def transform_job_data(message_data):
    print(f"Starting job data transformation for: {message_data}")

    api_source = message_data.get('api_source')
    filename = message_data.get('filename')
    bucket = message_data.get('bucket', BUCKET_NAME)
    
    if not api_source or not filename:
        print("Invalid message: missing required fields")
        return None
    
    df = download_json_from_gcs(bucket, filename)
    
    if df.empty:
        print(f"No data found in source file: {filename}")
        return None
    
    print(f"Downloaded {len(df)} records from {filename}")
    
    df_standardized = pd.DataFrame()
    df_standardized['source'] = api_source
    
    field_mappings = {
        'adzuna': {
            'job_title': 'title',
            'job_description': 'description',
            'job_url': 'redirect_url',
            'posted_date': 'created',
            'job_category': 'category.label',
            'job_type': 'contract_time',
            'company_name': 'company.display_name',
            'salary': 'salary_is_predicted',
            'salary_min': 'salary_min',
            'salary_max': 'salary_max'
        },
        'jooble': {
            'job_title': 'title',
            'job_description': 'snippet',
            'job_url': 'link',
            'posted_date': 'updated',
            'job_category': 'type',
            'job_type': 'type',
            'company_name': 'company',
            'salary': 'salary'
        },
        'muse': {
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
    
    if api_source in field_mappings:
        mapping = field_mappings[api_source]
        
        for new_col, original_col in mapping.items():
            if new_col not in ['salary_min', 'salary_max']:
                if '.' in original_col:
                    parts = original_col.split('.')
                    if parts[0] in df.columns:
                        if parts[0] == 'categories[0]' and 'categories' in df.columns:
                            df_standardized[new_col] = df['categories'].apply(
                                lambda x: x[0].get('name') if isinstance(x, list) and len(x) > 0 and 'name' in x[0] else None
                            )
                        elif parts[0] == 'refs' and 'refs' in df.columns:
                            df_standardized[new_col] = df['refs'].apply(
                                lambda x: x.get('landing_page') if isinstance(x, dict) and 'landing_page' in x else None
                            )
                        else:
                            df_standardized[new_col] = df[parts[0]].apply(
                                lambda x: x.get(parts[1]) if isinstance(x, dict) and parts[1] in x else None
                            )
                elif original_col in df.columns:
                    df_standardized[new_col] = df[original_col]
                else:
                    df_standardized[new_col] = None
        
        # Handle Adzuna salary separately
        if api_source == 'adzuna' and 'salary_min' in mapping and 'salary_max' in mapping:
            min_col = mapping['salary_min']
            max_col = mapping['salary_max']

            if min_col in df.columns and max_col in df.columns:
                df_standardized['salary'] = df[min_col].apply(lambda x: f"${x}" if pd.notna(x) else "").astype(str) + \
                                        ' - ' + \
                                        df[max_col].apply(lambda x: f"${x}" if pd.notna(x) else "").astype(str)
                df_standardized['salary'] = df_standardized['salary'].str.replace('$ - $', '', regex=False)
                df_standardized['salary'] = df_standardized['salary'].str.replace('$nan', '', regex=False)
                df_standardized['salary'] = df_standardized['salary'].str.replace('nan$', '', regex=False)
                df_standardized['salary'] = df_standardized['salary'].str.replace(' - ', '', regex=False)

        output_filename = f"transformed_{api_source}_jobs.json"
        upload_success = upload_to_gcs(df_standardized, output_filename, bucket)
        
        if upload_success:
            print(f"Transformation complete for {api_source}. Result saved to {output_filename}")
            return df_standardized
        else:
            print(f"Failed to upload transformed data for {api_source}")
            return None
    else:
        print(f"Unknown API source: {api_source}")
        return None

@app.route('/', methods=['GET'])
def home():
    return {'status': 'Job transform service is running'}, 200

@app.route('/pubsub', methods=['POST'])
def pubsub_handler():
    try:
        envelope = flask.request.get_json()
        
        if not envelope:
            return "No Pub/Sub message received", 400
            
        if not isinstance(envelope, dict) or 'message' not in envelope:
            return "Invalid Pub/Sub message format", 400
            
        pubsub_message = envelope['message']
        
        if 'data' in pubsub_message:
            message_data_str = base64.b64decode(pubsub_message['data']).decode('utf-8')
            message_data = json.loads(message_data_str)
            
            result = transform_job_data(message_data)
            
            if result is not None:
                return {
                    'status': 'success',
                    'message': f"Successfully transformed job data for {message_data.get('api_source')}"
                }, 200
            else:
                return {
                    'status': 'error',
                    'message': f"Failed to transform job data for {message_data.get('api_source')}"
                }, 500
        else:
            print("Invalid Pub/Sub message: missing data")
            return "Invalid message format", 400
    except Exception as e:
        print(f"Error processing Pub/Sub message: {str(e)}")
        return f"Error: {str(e)}", 500

@app.route('/manual', methods=['POST'])
def manual_transform():
    try:
        message_data = flask.request.get_json()
        
        if not message_data:
            return "No data provided", 400
            
        result = transform_job_data(message_data)
        
        if result is not None:
            return {
                'status': 'success',
                'message': f"Successfully transformed job data for {message_data.get('api_source')}"
            }, 200
        else:
            return {
                'status': 'error',
                'message': f"Failed to transform job data for {message_data.get('api_source')}"
            }, 500
    except Exception as e:
        print(f"Error in manual transform: {str(e)}")
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)