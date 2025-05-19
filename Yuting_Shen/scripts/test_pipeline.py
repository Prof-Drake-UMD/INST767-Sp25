"""
Test script for running the complete pipeline.
Verifies that all components work together correctly.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
import warnings

# Add the src directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Yuting_Shen.src.pipeline.integration_pipeline import IntegrationPipeline
from Yuting_Shen.src.utils.logging_utils import setup_logger


def run_test():
    """Run a test of the complete pipeline."""
    # Set up logger
    logger = setup_logger(log_file='test_pipeline.log')
    logger.info("Starting pipeline test")

    # Initialize the integration pipeline
    youtube_api_key = os.environ.get('YOUTUBE_API_KEY')
    sportsdb_api_key = os.environ.get('SPORTSDB_API_KEY')


    if not youtube_api_key:
        logger.warning("YOUTUBE_API_KEY environment variable not set. YouTube pipeline may fail.")


    # Configure trends with caching
    trends_config = {
        'cache_dir': "data/trends_cache",
        'cache_max_age_days': 7,
        'request_interval': 20,  # Shorter interval for testing
        'retries': 3,
        'backoff_factor': 2,
        'session_cooldown': 60   # Shorter cooldown for testing
    }

    pipeline = IntegrationPipeline(
        youtube_api_key=youtube_api_key,
        trends_config=trends_config,
        sportsdb_api_key= sportsdb_api_key,

    )

    # Run the pipeline with test data
    league_id = "4391"  #NFL
    team_search = "Kansas City Chiefs"
    youtube_query = "Kansas City Chiefs highlights"
    trend_keywords = ["Kansas City Chiefs", "NFL", "Football"]

    # Use properly formatted dates for YouTube API (RFC 3339)
    # Either provide dates with Z suffix or don't provide dates at all

    # Videos from the last 60 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)

    # Format in RFC 3339 format with Z
    published_after = start_date.strftime('%Y-%m-%dT00:00:00Z')
    published_before = end_date.strftime('%Y-%m-%dT23:59:59Z')

    start_time = time.time()

    try:
        # Pass cache_discovery=False to suppress googleapiclient warning
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            results = pipeline.run(
                league_id=league_id,
                team_search=team_search,
                youtube_query=youtube_query,
                trend_keywords=trend_keywords,
                max_results=5  # Limit to 5 videos for testing
            )

        duration = time.time() - start_time

        # Check if pipeline succeeded
        if results.get('status') == 'success':
            logger.info(f"Pipeline test completed successfully in {duration:.2f} seconds")

            # Check if data was generated
            data_counts = {
                'events': len(pipeline.events_data),
                'teams': len(pipeline.teams_data),
                'videos': len(pipeline.videos_data),
                'video_metrics': len(pipeline.video_metrics_data),
                'search_trends': len(pipeline.search_trends_data),
                'integrated_analyses': len(pipeline.integrated_analysis_data)
            }

            logger.info("Data counts:")
            for data_type, count in data_counts.items():
                logger.info(f"- {data_type}: {count}")

            # Check if CSV files were generated
            csv_files = results.get('csv_paths', {})
            logger.info("Generated CSV files:")
            for data_type, csv_path in csv_files.items():
                if csv_path and os.path.exists(csv_path):
                    logger.info(f"- {data_type}: {csv_path} (exists)")
                else:
                    logger.warning(f"- {data_type}: {csv_path} (missing)")

            # Save test results
            test_results = {
                'timestamp': datetime.now().isoformat(),
                'duration': duration,
                'status': 'success',
                'data_counts': data_counts,
                'csv_files': csv_files
            }

            with open('test_results.json', 'w') as f:
                json.dump(test_results, f, indent=2)

            logger.info("Test results saved to test_results.json")
            return True
        else:
            logger.error("Pipeline test failed")
            logger.error(f"Errors: {results.get('errors', [])}")
            return False
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Pipeline test failed with exception: {str(e)}")
        logger.exception(e)
        return False


if __name__ == "__main__":
    run_test()