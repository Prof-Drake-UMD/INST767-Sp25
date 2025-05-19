# Create the main function file
import os
import json
import logging
from datetime import datetime
import functions_framework
from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import your transformer modules
from sports_transformer import transform_events
from youtube_transformer import transform_videos, transform_video_metrics
from trends_transformer import transform_search_trends
from integration_transformer import create_integrated_events_analysis
from data_validator import validate_events, validate_videos, validate_video_metrics, validate_search_trends, \
    validate_integrated_analysis
from data_writer import write_to_csv


def download_from_gcs(bucket_name, blob_name):
    """Download a file from Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        contents = blob.download_as_string()
        return json.loads(contents)
    except Exception as e:
        logger.error(f"Error downloading from GCS: {str(e)}")
        return None


def upload_to_gcs(data, bucket_name, blob_name):
    """Upload data to Google Cloud Storage."""
    try:
        storage_client = storage.Client()

        # Create bucket if it doesn't exist
        try:
            bucket = storage_client.get_bucket(bucket_name)
        except Exception:
            bucket = storage_client.create_bucket(bucket_name)

        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        return f"gs://{bucket_name}/{blob_name}"
    except Exception as e:
        logger.error(f"Error uploading to GCS: {str(e)}")
        return None


@functions_framework.http
def transform_data(request):
    """
    Cloud Function HTTP entry point to transform data from multiple sources.
    """
    logger.info("Transform function started")

    # Parse request parameters
    request_json = request.get_json(silent=True)

    if not request_json:
        logger.error("No input data provided")
        return {"error": "No input data provided"}, 400

    # Extract file paths from request
    sports_file_path = request_json.get('sports_file_path')
    youtube_file_path = request_json.get('youtube_file_path')
    trends_file_path = request_json.get('trends_file_path')

    logger.info(f"Processing files: sports={sports_file_path}, youtube={youtube_file_path}, trends={trends_file_path}")

    # Check if at least sports data is provided
    if not sports_file_path:
        logger.error("Sports data file path is required")
        return {"error": "Sports data file path is required"}, 400

    # Parse GCS paths
    def parse_gcs_path(path):
        if not path or not path.startswith('gs://'):
            return None, None
        parts = path[5:].split('/', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    sports_bucket, sports_blob = parse_gcs_path(sports_file_path)
    youtube_bucket, youtube_blob = parse_gcs_path(youtube_file_path)
    trends_bucket, trends_blob = parse_gcs_path(trends_file_path)

    # Download data from GCS
    logger.info("Downloading data from Cloud Storage")
    sports_data = None
    youtube_data = None
    trends_data = None

    if sports_bucket and sports_blob:
        sports_data = download_from_gcs(sports_bucket, sports_blob)
        logger.info(f"Downloaded sports data: {len(str(sports_data))} bytes")

    if youtube_bucket and youtube_blob:
        youtube_data = download_from_gcs(youtube_bucket, youtube_blob)
        logger.info(f"Downloaded YouTube data: {len(str(youtube_data))} bytes")

    if trends_bucket and trends_blob:
        trends_data = download_from_gcs(trends_bucket, trends_blob)
        logger.info(f"Downloaded trends data: {len(str(trends_data))} bytes")

    if not sports_data:
        logger.error("Failed to download sports data")
        return {"error": "Failed to download sports data"}, 500

    # Transform data using your existing transform functions
    try:
        logger.info("Starting data transformation")

        # Transform sports data
        logger.info("Transforming sports data")
        transformed_events = transform_events(sports_data)
        valid_events, events_errors = validate_events(transformed_events)
        logger.info(f"Transformed {len(valid_events)} events, found {len(events_errors)} errors")

        # Transform YouTube data
        transformed_videos = []
        valid_videos = []
        video_metrics = []
        if youtube_data:
            logger.info("Transforming YouTube data")
            transformed_videos = transform_videos(
                youtube_data.get('search_results', []),
                youtube_data.get('video_details', {}),
                events_data=valid_events
            )
            valid_videos, videos_errors = validate_videos(transformed_videos)
            logger.info(f"Transformed {len(valid_videos)} videos, found {len(videos_errors)} errors")

            # Transform video metrics
            video_metrics = transform_video_metrics(youtube_data.get('video_details', {}))
            valid_metrics, metrics_errors = validate_video_metrics(video_metrics)
            logger.info(f"Transformed {len(valid_metrics)} video metrics, found {len(metrics_errors)} errors")

        # Transform trends data
        transformed_trends = []
        valid_trends = []
        if trends_data:
            logger.info("Transforming trends data")
            interest_over_time = trends_data.get('interest_over_time', {})
            transformed_trends = transform_search_trends(interest_over_time)
            valid_trends, trends_errors = validate_search_trends(transformed_trends)
            logger.info(f"Transformed {len(valid_trends)} trend data points, found {len(trends_errors)} errors")

        # Create integrated analysis
        logger.info("Creating integrated analysis")
        integrated_analyses = create_integrated_events_analysis(
            valid_events,
            valid_videos,
            video_metrics,
            valid_trends
        )
        valid_analyses, analyses_errors = validate_integrated_analysis(integrated_analyses)
        logger.info(f"Created {len(valid_analyses)} integrated analyses, found {len(analyses_errors)} errors")

    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        return {"error": f"Transformation failed: {str(e)}"}, 500

    # Save transformed data to Cloud Storage
    logger.info("Saving transformed data to Cloud Storage")

    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    if not project_id:
        project_id = request_json.get('project_id')
        if not project_id:
            logger.warning("No project ID found, using default bucket names")
            project_id = "your-project-id"

    # Create timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save results to GCS
    gcs_paths = {}

    # Save events data
    if valid_events:
        bucket_name = f"{project_id}-processed-data"
        blob_name = f"events/events_{timestamp}.json"
        path = upload_to_gcs(valid_events, bucket_name, blob_name)
        if path:
            gcs_paths['events'] = path
            logger.info(f"Saved events data to {path}")

    # Save videos data
    if valid_videos:
        bucket_name = f"{project_id}-processed-data"
        blob_name = f"videos/videos_{timestamp}.json"
        path = upload_to_gcs(valid_videos, bucket_name, blob_name)
        if path:
            gcs_paths['videos'] = path
            logger.info(f"Saved videos data to {path}")

    # Save video metrics
    if video_metrics:
        bucket_name = f"{project_id}-processed-data"
        blob_name = f"metrics/metrics_{timestamp}.json"
        path = upload_to_gcs(video_metrics, bucket_name, blob_name)
        if path:
            gcs_paths['metrics'] = path
            logger.info(f"Saved video metrics to {path}")

    # Save trends data
    if valid_trends:
        bucket_name = f"{project_id}-processed-data"
        blob_name = f"trends/trends_{timestamp}.json"
        path = upload_to_gcs(valid_trends, bucket_name, blob_name)
        if path:
            gcs_paths['trends'] = path
            logger.info(f"Saved trends data to {path}")

    # Save integrated analysis
    if valid_analyses:
        bucket_name = f"{project_id}-processed-data"
        blob_name = f"integrated/integrated_{timestamp}.json"
        path = upload_to_gcs(valid_analyses, bucket_name, blob_name)
        if path:
            gcs_paths['integrated'] = path
            logger.info(f"Saved integrated analysis to {path}")

    # Also create local CSV files for reference (optional)
    try:
        if valid_events:
            events_csv = write_to_csv(valid_events, 'events', 'data/processed')
            logger.info(f"Wrote events CSV: {events_csv}")

        if valid_videos:
            videos_csv = write_to_csv(valid_videos, 'videos', 'data/processed')
            logger.info(f"Wrote videos CSV: {videos_csv}")

        if valid_trends:
            trends_csv = write_to_csv(valid_trends, 'trends', 'data/processed')
            logger.info(f"Wrote trends CSV: {trends_csv}")

        if valid_analyses:
            analyses_csv = write_to_csv(valid_analyses, 'integrated_analyses', 'data/processed')
            logger.info(f"Wrote integrated analyses CSV: {analyses_csv}")
    except Exception as e:
        logger.warning(f"Could not write CSV files: {str(e)}")

    # Prepare response
    response = {
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "counts": {
            "events": len(valid_events),
            "videos": len(valid_videos),
            "trends": len(valid_trends),
            "integrated": len(valid_analyses)
        },
        "gcs_paths": gcs_paths
    }

    logger.info("Transform function completed successfully")
    return response
