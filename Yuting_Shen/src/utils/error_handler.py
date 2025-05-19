"""
Main pipeline runner script with enhanced logging and error handling.
Orchestrates the complete data pipeline by calling the integration pipeline.
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime
import logging

# Add the src directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import pipeline modules
from src.pipeline.integration_pipeline import IntegrationPipeline
from src.utils.logging_utils import setup_logger, log_pipeline_start, log_pipeline_end
from src.utils.error_handler import handle_error, wrap_with_error_handling

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run the data pipeline')

    parser.add_argument('--league-id', type=str, required=True,
                        help='ID of the league to process')

    parser.add_argument('--team-search', type=str,
                        help='Team name to search for')

    parser.add_argument('--youtube-query', type=str,
                        help='Search query for YouTube videos')

    parser.add_argument('--trend-keywords', type=str, nargs='+',
                        help='List of keywords for Google Trends')

    parser.add_argument('--max-results', type=int, default=10,
                        help='Maximum number of YouTube videos to fetch')

    parser.add_argument('--youtube-api-key', type=str,
                        help='YouTube API key (can also be set as YOUTUBE_API_KEY env var)')

    parser.add_argument('--output-dir', type=str, default='data/processed',
                        help='Directory for output CSV files')

    parser.add_argument('--log-dir', type=str, default='logs',
                        help='Directory for log files')

    parser.add_argument('--error-dir', type=str, default='data/errors',
                        help='Directory for error logs')

    parser.add_argument('--debug', action='store_true',
                        help='Enable debug logging')

    return parser.parse_args()

def save_results(results, output_dir='data/processed'):
    """
    Save pipeline results to a JSON file.

    Args:
        results (dict): Pipeline results
        output_dir (str): Output directory

    Returns:
        str: Path to the results file
    """
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate results filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(output_dir, f"pipeline_results_{timestamp}.json")

    # Write results to file
    with open(file_path, 'w') as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved pipeline results to {file_path}")
    return file_path

@wrap_with_error_handling
def run_pipeline(args, logger):
    """
    Run the pipeline with error handling.

    Args:
        args (argparse.Namespace): Command line arguments
        logger (logging.Logger): Logger

    Returns:
        dict: Pipeline results
    """
    start_time = time.time()

    # Log pipeline start
    log_pipeline_start(
        logger,
        "Integration",
        league_id=args.league_id,
        team_search=args.team_search,
        youtube_query=args.youtube_query,
        trend_keywords=args.trend_keywords,
        max_results=args.max_results
    )

    # Get YouTube API key from arguments or environment variable
    youtube_api_key = args.youtube_api_key or os.environ.get('YOUTUBE_API_KEY')

    if not youtube_api_key:
        logger.warning("YouTube API key not provided. YouTube pipeline may fail.")

    # Initialize the integration pipeline
    pipeline = IntegrationPipeline(youtube_api_key)

    # Run the pipeline
    results = pipeline.run(
        league_id=args.league_id,
        team_search=args.team_search,
        youtube_query=args.youtube_query,
        trend_keywords=args.trend_keywords,
        max_results=args.max_results
    )

    # Calculate duration
    duration = time.time() - start_time

    # Log pipeline end
    log_pipeline_end(
        logger,
        "Integration",
        results.get('status', 'unknown'),
        duration,
        events_count=len(pipeline.events_data),
        teams_count=len(pipeline.teams_data),
        videos_count=len(pipeline.videos_data),
        metrics_count=len(pipeline.video_metrics_data),
        trends_count=len(pipeline.search_trends_data),
        integrated_count=len(pipeline.integrated_analysis_data)
    )

    # Add duration to results
    results['duration'] = duration

    return results

def main():
    """Main function to run the pipeline."""
    # Parse command line arguments
    args = parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.debug else logging.INFO

    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(args.log_dir, f"pipeline_{timestamp}.log")

    # Set up logger
    global logger
    logger = setup_logger(log_file, log_level)

    logger.info("Starting pipeline run")

    try:
        # Run the pipeline
        results = run_pipeline(args, logger)

        # Save results
        results_file = save_results(results, args.output_dir)

        # Print summary
        if results.get('status') == 'success':
            logger.info("Pipeline run completed successfully")
            return 0
        else:
            logger.error("Pipeline run failed")
            return 1
    except Exception as e:
        # Handle unexpected errors
        error_details = handle_error(e, log_file, args.error_dir)
        logger.error("Pipeline run failed with unexpected error")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)