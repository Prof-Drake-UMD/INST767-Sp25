"""
Script to deploy YouTube API ingestion function to Google Cloud Functions.
"""

import os
import argparse
import tempfile
import shutil
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def deploy_youtube_function(project_id, region="us-central1"):
    """
    Deploy the YouTube API ingestion function to Cloud Functions.
    """
    # Create a temporary directory for function code
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy the necessary files to the temp directory
        shutil.copy("src/ingest/youtube_api.py", os.path.join(temp_dir, "youtube_api.py"))

        import textwrap

        # Create a new main.py file that sets up the Flask server
        with open(os.path.join(temp_dir, "main.py"), "w") as f:
            f.write(textwrap.dedent("""
                import os
                import flask
                import functions_framework
                from youtube_api import YouTubeAPI, search_sports_videos
                from google.cloud import storage
                from google.cloud import pubsub_v1
                import json
                from datetime import datetime

                # Create a Flask app for the function
                app = flask.Flask(__name__)

                @app.route("/", methods=["POST", "GET"])
                def http_function():
                    return search_sports_videos(flask.request)

                if __name__ == "__main__":
                    # This is used when running locally
                    port = int(os.environ.get("PORT", 8080))
                    app.run(host="0.0.0.0", port=port, debug=True)
            """))

            # Create a requirements.txt file
        with open(os.path.join(temp_dir, "requirements.txt"), "w") as f:
            f.write("google-api-python-client==2.97.0\n")
            f.write("google-auth==2.22.0\n")
            f.write("google-auth-httplib2==0.1.0\n")
            f.write("google-cloud-storage==2.10.0\n")
            f.write("google-cloud-pubsub==2.18.4\n")
            f.write("python-dotenv==1.0.0\n")
            f.write("flask==2.0.1\n")
            f.write("functions-framework==3.0.0\n")

        # Deploy the function
        function_name = "ingest-youtube-data"
        entry_point = "search_sports_videos"  # The function to trigger
        deploy_cmd = f"""
            gcloud functions deploy {function_name} \\
              --project={project_id} \\
              --region={region} \\
              --runtime=python310 \\
              --source={temp_dir} \\
              --gen2 \\
              --entry-point={entry_point} \\
              --trigger-http \\
              --memory=256MB \\
              --timeout=540s \\
              --allow-unauthenticated
            """

        logger.info(f"Deploying function {function_name}...")
        result = subprocess.run(deploy_cmd, shell=True)

        if result.returncode == 0:
            logger.info(f"Successfully deployed {function_name}")
            # Get the function URL
            url_cmd = f"gcloud functions describe {function_name} --region={region} --format='value(httpsTrigger.url)'"
            url_result = subprocess.run(url_cmd, shell=True, capture_output=True, text=True)
            function_url = url_result.stdout.strip()
            logger.info(f"Function URL: {function_url}")
        else:
            logger.error(f"Failed to deploy {function_name}")



def main():
    parser = argparse.ArgumentParser(description="Deploy YouTube API ingestion function")
    parser.add_argument("--project-id", required=True, help="Google Cloud Project ID")
    parser.add_argument("--region", default="us-central1", help="Google Cloud region")

    args = parser.parse_args()
    deploy_youtube_function(args.project_id, args.region)


if __name__ == "__main__":
    main()