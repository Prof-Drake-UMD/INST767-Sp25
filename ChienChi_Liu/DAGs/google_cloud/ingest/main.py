import os
import json
import datetime
import flask
from google.cloud import storage
from google.cloud import pubsub_v1
from muse_api import MuseConnector
from adzuna_api import AdzunaConnector
from jooble_api import JoobleConnector

muse_api_key = os.environ.get('MUSE_API_KEY')
adzuna_api_id = os.environ.get('ADZUNA_APP_ID')
adzuna_api_key = os.environ.get('ADZUNA_APP_KEY')
jooble_api_key = os.environ.get('JOOBLE_API_KEY')
PROJECT_ID = os.environ.get('PROJECT_ID')
BUCKET_NAME = f"job-data-{PROJECT_ID}"
JOBS_TOPIC = 'jobs-data-topic'

app = flask.Flask(__name__)

def upload_to_gcs(data, filename):
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
        print(f"File {filename} uploaded to {BUCKET_NAME}")
        return True
    except Exception as e:
        print(f"Error uploading to GCS: {str(e)}")
        return False

def publish_to_pubsub(api_name, data, timestamp):
    filename = f"{api_name}_jobs.json"

    if upload_to_gcs(data, filename):
        message_data = {
            "api_source": api_name,
            "filename": filename,
            "record_count": len(data),
            "timestamp": timestamp,
            "bucket": BUCKET_NAME
        }

        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(PROJECT_ID, JOBS_TOPIC)
            data_bytes = json.dumps(message_data).encode("utf-8")

            future = publisher.publish(topic_path, data_bytes)
            message_id = future.result()
            print(f"Published message {message_id} for {api_name} job data")
            print(f"Message data for {api_name}: {message_data}")
            return True
        except Exception as e:
            print(f"Error publishing message for {api_name}: {str(e)}")
            return False
        
    else:
        print(f"Failed to upload {api_name} data to GCS")
        return False

def collect_jobs():
    timestamp = datetime.datetime.now().isoformat()
    results = {
        "success": 0,
        "total": 3,
        "apis_processed": []
    }
    
    # Adzuna API
    try: 
        if adzuna_api_id and adzuna_api_key:
            adzuna = AdzunaConnector(adzuna_api_id, adzuna_api_key)
            keywords = ["software", "data", "devops", "engineer", "IT", "developer", "designer", "manager"]
            adzuna_jobs = adzuna.extract_jobs(keywords=keywords)
            if publish_to_pubsub("adzuna", adzuna_jobs, timestamp):
                results["success"] += 1
                results["apis_processed"].append("adzuna")
        else:
            print("Missing Adzuna API credentials")
    except Exception as e:
        print(f"Error collecting Adzuna jobs: {str(e)}")
    
    # Jooble API
    try:
        if jooble_api_key:
            jooble = JoobleConnector(jooble_api_key)
            jooble_jobs = jooble.extract_jobs(
                keywords=["engineer", "designer"], 
                locations=["remote"], 
                limit=100
            )
            if publish_to_pubsub("jooble", jooble_jobs, timestamp):
                results["success"] += 1
                results["apis_processed"].append("jooble")
        else:
            print("Missing Jooble API key")
    except Exception as e:
        print(f"Error collecting Jooble jobs: {str(e)}")
    
    # Muse API
    try:
        if muse_api_key:
            muse = MuseConnector(muse_api_key)
            categories = ["ux", "design", "management"]
            muse_jobs = muse.extract_jobs(categories=categories)
            if publish_to_pubsub("muse", muse_jobs, timestamp):
                results["success"] += 1
                results["apis_processed"].append("muse")
        else:
            print("Missing Muse API key")
    except Exception as e:
        print(f"Error collecting Muse jobs: {str(e)}")
    
    return results

@app.route('/', methods=['GET'])
def home():
    return {'status': 'Job fetch service is running'}, 200

@app.route('/fetch', methods=['POST'])
def fetch_handler():
    try:
        results = collect_jobs()
        return {
            'status': 'success',
            'message': f"Job collection completed. Successfully published {results['success']} out of {results['total']} APIs.",
            'details': results
        }, 200
    except Exception as e:
        print(f"Error in fetch_handler: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }, 500

@app.route('/pubsub', methods=['POST'])
def pubsub_handler():
    try:
        envelope = flask.request.get_json()
        if not envelope:
            return "No Pub/Sub message received", 400
        if not isinstance(envelope, dict) or 'message' not in envelope:
            return "Invalid Pub/Sub message format", 400
        results = collect_jobs()
        return {
            'status': 'success',
            'message': f"Job collection completed. Successfully published {results['success']} out of {results['total']} APIs.",
            'details': results
        }, 200
    except Exception as e:
        print(f"Error in pubsub_handler: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }, 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)