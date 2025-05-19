
"""
Simple web server for Cloud Run to trigger data transformation.
"""

import sys
import os
# Add the current directory to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from flask import Flask, request, jsonify
import logging
import threading
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Track job status
jobs = {}

def run_pipeline_job(job_id, params):
    """Run the pipeline in a background thread."""
    try:
        logger.info(f"Starting job {job_id} with params: {params}")
        jobs[job_id]['status'] = 'running'
        
        # Get parameters
        league_id = params.get('league_id', '4391')
        team_search = params.get('team_search')
        youtube_query = params.get('youtube_query')
        trend_keywords = params.get('trend_keywords', [])
        max_results = int(params.get('max_results', 10))
        
        # Import your pipeline here to avoid circular imports
        from src.pipeline.integration_pipeline import IntegrationPipeline
        
        # Get API key from environment
        youtube_api_key = os.environ.get('YOUTUBE_API_KEY')
        
        # Initialize and run pipeline
        pipeline = IntegrationPipeline(youtube_api_key=youtube_api_key)
        results = pipeline.run(
            league_id=league_id,
            team_search=team_search,
            youtube_query=youtube_query,
            trend_keywords=trend_keywords,
            max_results=max_results
        )
        
        # Update job status
        jobs[job_id]['status'] = 'completed'
        jobs[job_id]['results'] = {
            'status': results.get('status'),
            'csv_paths': results.get('csv_paths', {}),
            'errors': results.get('errors', [])
        }
        logger.info(f"Job {job_id} completed with status: {results.get('status')}")
        
    except Exception as e:
        logger.error(f"Error in job {job_id}: {str(e)}")
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['error'] = str(e)

@app.route('/', methods=['GET'])
def home():
    """Root endpoint for health checks."""
    return jsonify({
        'status': 'ok',
        'service': 'sports-data-transform',
        'message': 'Service is running. POST to /transform to start a data processing job.'
    })

@app.route('/transform', methods=['POST'])
def transform():
    """Endpoint to trigger data transformation."""
    # Generate a job ID
    job_id = f"job_{int(time.time())}"
    
    # Get parameters from request
    params = request.get_json() or {}
    
    # Create job entry
    jobs[job_id] = {
        'id': job_id,
        'status': 'queued',
        'params': params,
        'created_at': time.time()
    }
    
    # Start processing in a background thread
    thread = threading.Thread(target=run_pipeline_job, args=(job_id, params))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'status': 'accepted',
        'job_id': job_id,
        'message': 'Data transformation job started'
    })

@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get the status of a specific job."""
    if job_id not in jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    return jsonify(jobs[job_id])

@app.route('/jobs', methods=['GET'])
def list_jobs():
    """List all jobs."""
    return jsonify(list(jobs.values()))

if __name__ == '__main__':
    # Use the PORT environment variable provided by Cloud Run
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
