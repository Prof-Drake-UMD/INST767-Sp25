#!/bin/bash

# Script to upload Python modules and DAGs, and optionally set Airflow Variables.
set -e # Exit immediately if a command exits with a non-zero status.
set -o pipefail # Causes a pipeline to return the exit status of the last command in the pipe that failed.

# --- Configuration & Environment Loading ---
echo "[INFO] --- Starting Composer Deployment Script ---"

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
    echo "[INFO] Loading environment variables from .env file..."
    set -o allexport
    # shellcheck source=/dev/null
    source .env
    set +o allexport
    echo "[INFO] .env file processed."
else
    echo "[WARNING] .env file not found. Please ensure it exists and contains necessary configurations, or that variables are set externally."
fi

# Prompt for or use environment variables for Composer details
if [ -z "$COMPOSER_ENV_NAME" ]; then
    read -r -p "Enter your Cloud Composer environment name: " COMPOSER_ENV_NAME
    if [ -z "$COMPOSER_ENV_NAME" ]; then echo "[ERROR] Composer environment name is required."; exit 1; fi
fi

if [ -z "$COMPOSER_LOCATION" ]; then
    read -r -p "Enter the region/location of your Composer environment (e.g., us-central1): " COMPOSER_LOCATION
    if [ -z "$COMPOSER_LOCATION" ]; then echo "[ERROR] Composer environment location is required."; exit 1; fi
fi

# GCP_PROJECT_ID will be inferred from gcloud config if not explicitly set in .env
# but it's good practice to have it for clarity.
CURRENT_GCP_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$GCP_PROJECT_ID" ] && [ -n "$CURRENT_GCP_PROJECT" ]; then
    echo "[INFO] GCP_PROJECT_ID not found in .env, using current gcloud project: $CURRENT_GCP_PROJECT"
    GCP_PROJECT_ID="$CURRENT_GCP_PROJECT"
elif [ -z "$GCP_PROJECT_ID" ]; then
    read -r -p "Enter your GCP Project ID: " GCP_PROJECT_ID
    if [ -z "$GCP_PROJECT_ID" ]; then echo "[ERROR] GCP Project ID is required."; exit 1; fi
fi

echo "[INFO] Using Configuration:"
echo "  Composer Env Name: $COMPOSER_ENV_NAME"
echo "  Composer Location: $COMPOSER_LOCATION"
echo "  GCP Project ID:    $GCP_PROJECT_ID"
echo "----------------------------------------------------------------------"

# === Step 1: Get Composer DAGs GCS Bucket Path ===
echo "[INFO] Step 1: Getting Composer DAGs GCS bucket path..."
COMPOSER_DAGS_BUCKET_PATH=$(gcloud composer environments describe "$COMPOSER_ENV_NAME" \
    --project "$GCP_PROJECT_ID" \
    --location "$COMPOSER_LOCATION" \
    --format="value(config.dagGcsPrefix)")

if [ -z "$COMPOSER_DAGS_BUCKET_PATH" ]; then
    echo "[ERROR] Could not retrieve Composer DAGs GCS bucket path. Exiting."
    exit 1
fi
echo "[SUCCESS] Composer DAGs bucket path: $COMPOSER_DAGS_BUCKET_PATH"
echo "----------------------------------------------------------------------"

# === Step 2: Define and Prepare GCS Paths for Modules ===
echo "[INFO] Step 2: Preparing GCS paths for modules..."
COMPOSER_SRC_PATH="${COMPOSER_DAGS_BUCKET_PATH%/dags}/src"
# COMPOSER_BOOK_PIPELINE_PATH="${COMPOSER_SRC_PATH}/book_pipeline_consumer_func" # Not used directly later

# Create an empty file to ensure the src directory exists
echo "[INFO] Ensuring 'src' directory exists in GCS bucket (${COMPOSER_SRC_PATH}/)..."
echo "" > empty.txt
gsutil -q cp empty.txt "${COMPOSER_SRC_PATH}/.placeholder" # -q for quiet
rm empty.txt
echo "[SUCCESS] 'src' directory prepared."
echo "----------------------------------------------------------------------"

# === Step 3: Upload DAG File ===
echo "[INFO] Step 3: Uploading DAG file (dags/book_data_ingestion_dag.py)..."
gsutil -q cp dags/book_data_ingestion_dag.py "${COMPOSER_DAGS_BUCKET_PATH}/"
echo "[SUCCESS] DAG file uploaded."
echo "----------------------------------------------------------------------"

# === Step 4: Upload Python Modules ===
echo "[INFO] Step 4: Uploading 'book_pipeline_consumer_func' modules to ${COMPOSER_SRC_PATH}/..."
if [ -d "book_pipeline_consumer_func" ]; then
    gsutil -q -m cp -r book_pipeline_consumer_func "${COMPOSER_SRC_PATH}/"
    echo "[SUCCESS] 'book_pipeline_consumer_func' modules uploaded."
else
    echo "[WARNING] Directory 'book_pipeline_consumer_func' not found. Skipping module upload."
fi
echo "----------------------------------------------------------------------"

# === Step 5: Create and Upload Import Helper ===
echo "[INFO] Step 5: Creating and uploading 'import_helper.py'..."
cat > import_helper.py << EOL
"""Helper module to import modules from the Composer src directory"""
import os
import sys
from pathlib import Path

# Add src directory to Python path
dag_file_path = Path(os.path.abspath(__file__))
# src_path becomes /home/airflow/gcs/dags/../src -> /home/airflow/gcs/src
src_path = dag_file_path.parent.parent / "src"
if src_path.exists() and str(src_path) not in sys.path:
    sys.path.append(str(src_path))
EOL

gsutil -q cp import_helper.py "${COMPOSER_DAGS_BUCKET_PATH}/"
echo "[SUCCESS] 'import_helper.py' uploaded."
echo "----------------------------------------------------------------------"

# === Step 6: Modify DAG File to Use Import Helper (Idempotent) ===
echo "[INFO] Step 6: Modifying DAG file to use 'import_helper.py' (if not already done)..."
TMP_DAG_FILE=$(mktemp)
TARGET_GCS_DAG_FILE="${COMPOSER_DAGS_BUCKET_PATH}/book_data_ingestion_dag.py"

# Download the current DAG from GCS
gsutil -q cp "$TARGET_GCS_DAG_FILE" "$TMP_DAG_FILE"

# Check if import_helper is already imported
if ! grep -q -E "^import[[:space:]]+import_helper" "$TMP_DAG_FILE"; then
    echo "[INFO] Adding 'import import_helper' to DAG file..."
    # Use printf and cat for a robust way to add the line at the beginning
    printf "import import_helper\n%s" "$(cat "$TMP_DAG_FILE")" > "$TMP_DAG_FILE.new" && mv "$TMP_DAG_FILE.new" "$TMP_DAG_FILE"
    
    # Upload the modified DAG file
    gsutil -q cp "$TMP_DAG_FILE" "$TARGET_GCS_DAG_FILE"
    echo "[SUCCESS] DAG file modified and re-uploaded."
else
    echo "[INFO] 'import import_helper' already exists in DAG file. Skipping modification."
fi
rm -f "$TMP_DAG_FILE" # Clean up the temp file
echo "----------------------------------------------------------------------"

# === Step 7: Attempt to Set Airflow Variables ===
echo "[INFO] Step 7: Attempting to set Airflow Variables for function names..."
AIRFLOW_VARIABLES_TO_SET=""
CONFIG_UPDATED=false

if [ -n "$NYT_INGEST_FUNCTION_NAME" ]; then
    AIRFLOW_VARIABLES_TO_SET="${AIRFLOW_VARIABLES_TO_SET}variables-nyt_ingest_function_name=${NYT_INGEST_FUNCTION_NAME},"
    echo "[INFO] Will attempt to set Airflow Variable 'nyt_ingest_function_name' to '$NYT_INGEST_FUNCTION_NAME'."
    CONFIG_UPDATED=true
else
    echo "[WARNING] Environment variable NYT_INGEST_FUNCTION_NAME not set. Skipping Airflow Variable update for NYT function."
fi

if [ -n "$OPEN_LIBRARY_INGEST_FUNCTION_NAME" ]; then
    AIRFLOW_VARIABLES_TO_SET="${AIRFLOW_VARIABLES_TO_SET}variables-open_library_ingest_function_name=${OPEN_LIBRARY_INGEST_FUNCTION_NAME},"
    echo "[INFO] Will attempt to set Airflow Variable 'open_library_ingest_function_name' to '$OPEN_LIBRARY_INGEST_FUNCTION_NAME'."
    CONFIG_UPDATED=true
else
    echo "[WARNING] Environment variable OPEN_LIBRARY_INGEST_FUNCTION_NAME not set. Skipping Airflow Variable update for Open Library function."
fi

if [ -n "$GOOGLE_BOOKS_INGEST_FUNCTION_NAME" ]; then
    AIRFLOW_VARIABLES_TO_SET="${AIRFLOW_VARIABLES_TO_SET}variables-google_books_ingest_function_name=${GOOGLE_BOOKS_INGEST_FUNCTION_NAME},"
    echo "[INFO] Will attempt to set Airflow Variable 'google_books_ingest_function_name' to '$GOOGLE_BOOKS_INGEST_FUNCTION_NAME'."
    CONFIG_UPDATED=true
else
    echo "[WARNING] Environment variable GOOGLE_BOOKS_INGEST_FUNCTION_NAME not set. Skipping Airflow Variable update for Google Books function."
fi

if $CONFIG_UPDATED; then
    # Remove trailing comma
    AIRFLOW_VARIABLES_TO_SET=${AIRFLOW_VARIABLES_TO_SET%,}
    echo "[INFO] Updating Airflow Variables in Composer. This may take a few minutes..."
    if gcloud composer environments update "$COMPOSER_ENV_NAME" \
        --project "$GCP_PROJECT_ID" \
        --location "$COMPOSER_LOCATION" \
        --update-airflow-configs="$AIRFLOW_VARIABLES_TO_SET"; then
        echo "[SUCCESS] Airflow Variables updated (or were already set to these values)."
    else
        echo "[ERROR] Failed to update Airflow Variables. Please check gcloud output and ensure variables are set correctly in the Airflow UI."
    fi
else
    echo "[INFO] No function name environment variables (NYT_INGEST_FUNCTION_NAME, etc.) found. Skipping Airflow Variable update step."
    echo "[IMPORTANT] Please ensure the following Airflow Variables are set correctly in your Composer UI (Admin -> Variables):"
    echo "  - nyt_ingest_function_name: (should be the deployed service name, e.g., nyt-ingest-http)"
    echo "  - open_library_ingest_function_name: (e.g., open-library-ingest-http)"
    echo "  - google_books_ingest_function_name: (e.g., google-books-ingest-http)"
fi
echo "----------------------------------------------------------------------"

echo "[INFO] --- Composer Deployment Script Finished ---"
echo "[INFO] Files uploaded to Composer GCS bucket."
echo "[INFO] If Airflow Variables were updated, it may take a moment for changes to be fully applied."
echo "[INFO] Check your Composer environment and Airflow UI for the DAG and variable states." 