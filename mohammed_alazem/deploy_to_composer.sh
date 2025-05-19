#!/bin/bash

# Script to deploy DAGs and custom code to Google Cloud Composer

set -e # Exit immediately if a command exits with a non-zero status.
# set -x # Print commands and their arguments as they are executed (for debugging)

echo "Starting Cloud Composer Deployment Script..."

# --- Configuration - Please fill these in or ensure they are correct ---
# Attempt to load variables from .env file if it exists
if [ -f ".env" ]; then
    echo "Loading environment variables from .env file..."
    set -o allexport # Export all variables sourced from the file
    source .env
    set +o allexport
else
    echo "WARNING: .env file not found. Configurations might not be set correctly."
fi

# Prompt for Composer Environment Details
if [ -z "$COMPOSER_ENV_NAME" ]; then
    echo "Composer environment name cannot be empty. Exiting."
    exit 1
fi

if [ -z "$COMPOSER_LOCATION" ]; then
    echo "Composer environment location cannot be empty. Exiting."
    exit 1
fi

# Project specific names
CUSTOM_PACKAGE_DIR="book_pipeline_consumer_func"
CUSTOM_PACKAGE_NAME="book_pipeline_consumer_func" # Should match the name in setup.py
DAGS_DIR="dags"
# Main DAG file we are deploying
BOOK_INGESTION_DAG_FILE_LOCAL="$DAGS_DIR/book_data_ingestion_dag.py"

# DAG ID for later reference
DAG_ID="book_data_ingestion_pipeline_direct_invoke" 

# --- 1. Package Custom Python Module ---
echo_section_header() {
    echo ""
    echo "---------------------------------------------------------------------"
    echo "$1"
    echo "---------------------------------------------------------------------"
}

echo_section_header "Step 1: Packaging custom Python module '$CUSTOM_PACKAGE_NAME'"

if [ ! -d "$CUSTOM_PACKAGE_DIR" ]; then
    echo "ERROR: Custom package directory '$CUSTOM_PACKAGE_DIR' not found. Exiting."
    exit 1
fi

# Create a basic setup.py if it doesn't exist
if [ ! -f "setup.py" ]; then
    echo "Creating a basic setup.py for '$CUSTOM_PACKAGE_NAME'..."
    cat > setup.py <<EOL
from setuptools import setup, find_packages

setup(
    name="$CUSTOM_PACKAGE_NAME",
    version="0.1.0",
    packages=find_packages(include=["$CUSTOM_PACKAGE_DIR", "$CUSTOM_PACKAGE_DIR.*"]), # Ensures submodules are included
    install_requires=[
        # 'google-cloud-bigquery',
        # 'google-cloud-pubsub',
        # 'requests',
        # 'apache-airflow-providers-google' # Important if your custom code uses it, though DAGs get it from Composer
    ],
    author="Mohamad", # Change this
    author_email="your.email@example.com", # Change this
    description="A custom package for the book data pipeline project.",
    url="" 
)
EOL
    echo "setup.py created. Please review and customize it if needed."
else
    echo "setup.py already exists. Using it."
fi

# Clean previous builds
rm -rf dist/ build/ *.egg-info
echo "Building wheel for $CUSTOM_PACKAGE_NAME..."

# First check if there's an __init__.py file and make sure it's properly formatted
if [ -f "$CUSTOM_PACKAGE_DIR/__init__.py" ]; then
    echo "Ensuring $CUSTOM_PACKAGE_DIR/__init__.py has proper namespace handling..."
    cat > "$CUSTOM_PACKAGE_DIR/__init__.py" <<EOL
# This package uses implicit namespace packages (PEP 420)
# No need for pkg_resources.declare_namespace
__version__ = "0.1.0"
EOL
fi

# Build the package
python setup.py sdist bdist_wheel
WHEEL_FILE_PATH=$(ls dist/*.whl | head -n 1) # Get the first wheel file found
if [ -z "$WHEEL_FILE_PATH" ] || [ ! -f "$WHEEL_FILE_PATH" ]; then
    echo "ERROR: Wheel file not found in dist/ directory after build. Exiting."
    exit 1
fi
echo "Custom package built: $WHEEL_FILE_PATH"

# --- Step 2: Get Composer DAGs GCS Bucket Path ---
echo_section_header "Step 2: Getting Composer DAGs GCS bucket path"
COMPOSER_DAGS_BUCKET_PATH=$(gcloud composer environments describe "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_LOCATION" \
    --format="value(config.dagGcsPrefix)")

if [ -z "$COMPOSER_DAGS_BUCKET_PATH" ]; then
    echo "ERROR: Could not retrieve Composer DAGs GCS bucket path for environment '$COMPOSER_ENV_NAME' in location '$COMPOSER_LOCATION'. Exiting."
    exit 1
fi
echo "Composer DAGs bucket path: $COMPOSER_DAGS_BUCKET_PATH"

# Get the GCS bucket path for the Python packages
GCS_BUCKET=$(echo "$COMPOSER_DAGS_BUCKET_PATH" | sed -E 's|(gs://[^/]+)/.*|\1|')
PLUGINS_FOLDER="${GCS_BUCKET}/plugins"
echo "Composer plugins folder for packages: $PLUGINS_FOLDER"

# --- Step 3: Upload DAGs to Composer's GCS Bucket ---
echo_section_header "Step 3: Uploading DAG files to Composer GCS bucket"

if [ -f "$BOOK_INGESTION_DAG_FILE_LOCAL" ]; then
    echo "Uploading $BOOK_INGESTION_DAG_FILE_LOCAL to $COMPOSER_DAGS_BUCKET_PATH/book_data_ingestion_dag.py ..."
    gsutil cp "$BOOK_INGESTION_DAG_FILE_LOCAL" "$COMPOSER_DAGS_BUCKET_PATH/book_data_ingestion_dag.py"
else
    echo "ERROR: Main DAG file $BOOK_INGESTION_DAG_FILE_LOCAL not found. Skipping its upload."
    exit 1 # This is critical, so exit
fi

# --- Step 4: Upload Custom Wheel and Requirements to GCS ---
echo_section_header "Step 4: Uploading custom wheel and requirements to GCS"

# First, check if we have a requirements.txt file
if [ ! -f "requirements.txt" ]; then
    echo "WARNING: No requirements.txt file found. Creating an empty one..."
    touch requirements.txt
fi

# Create a temporary directory to prepare our files
TEMP_DIR=$(mktemp -d)
echo "Creating temporary directory for requirements: $TEMP_DIR"

# Copy the wheel file to the temp directory
cp "$WHEEL_FILE_PATH" "$TEMP_DIR/"

# Create a requirements.txt file in the temp directory that includes our wheel
WHEEL_FILENAME=$(basename "$WHEEL_FILE_PATH")
cat requirements.txt > "$TEMP_DIR/requirements.txt"
echo "./$WHEEL_FILENAME" >> "$TEMP_DIR/requirements.txt"

# Upload the requirements.txt file to GCS
REQUIREMENTS_GCS_PATH="$PLUGINS_FOLDER/requirements.txt"
echo "Uploading requirements.txt to $REQUIREMENTS_GCS_PATH..."
gsutil cp "$TEMP_DIR/requirements.txt" "$REQUIREMENTS_GCS_PATH"

# Upload the wheel file to the same location
WHEEL_GCS_PATH="$PLUGINS_FOLDER/$WHEEL_FILENAME"
echo "Uploading $WHEEL_FILENAME to $WHEEL_GCS_PATH..."
gsutil cp "$WHEEL_FILE_PATH" "$WHEEL_GCS_PATH"

# Clean up the temporary directory
rm -rf "$TEMP_DIR"

# --- Step 5: Update Composer with PyPI Packages (Multiple Methods) ---
echo_section_header "Step 5: Updating Composer PyPI packages - will try multiple methods"

# Method 1: Skip local file method since path has spaces and causes PEP-508 errors
echo "METHOD 1: Skipping local file reference (path contains spaces)"
echo "❌ Local file reference method won't work due to spaces in path. Moving to Method 2..."

# Method 2: Using GCS path which doesn't have spaces
echo "METHOD 2: Using PEP-508 compliant GCS reference"
LOCAL_REQUIREMENTS_PATH="composer_requirements.txt"
cp requirements.txt "$LOCAL_REQUIREMENTS_PATH"
# Format: package_name @ gs://bucket/path/to/wheel
echo "$CUSTOM_PACKAGE_NAME @ $WHEEL_GCS_PATH" >> "$LOCAL_REQUIREMENTS_PATH"

echo "GCS requirements content:"
cat "$LOCAL_REQUIREMENTS_PATH"

echo "Attempting to update Composer environment with PEP-508 GCS reference..."
if gcloud composer environments update "$COMPOSER_ENV_NAME" \
    --location "$COMPOSER_LOCATION" \
    --update-pypi-packages-from-file "$LOCAL_REQUIREMENTS_PATH"; then
    echo "✅ SUCCESS: Updated Composer PyPI packages with PEP-508 GCS reference."
else
    echo "❌ Method 2 failed. Trying Method 3..."
    
    # Method 3: Upload to temporary GCS path that doesn't have spaces
    echo "METHOD 3: Using temporary location with clean path"
    TEMP_GCS_PATH="${GCS_BUCKET}/temp/${WHEEL_FILENAME}"
    echo "Uploading wheel to temporary location without spaces: $TEMP_GCS_PATH"
    gsutil cp "$WHEEL_FILE_PATH" "$TEMP_GCS_PATH"
    
    # Create clean requirements file
    echo "Creating clean requirements file..."
    echo "$CUSTOM_PACKAGE_NAME @ $TEMP_GCS_PATH" > "$LOCAL_REQUIREMENTS_PATH"
    
    echo "Clean requirements content:"
    cat "$LOCAL_REQUIREMENTS_PATH"
    
    echo "Attempting to update with clean GCS path..."
    if gcloud composer environments update "$COMPOSER_ENV_NAME" \
        --location "$COMPOSER_LOCATION" \
        --update-pypi-packages-from-file "$LOCAL_REQUIREMENTS_PATH"; then
        echo "✅ SUCCESS: Updated Composer with clean GCS path reference."
    else
        echo "❌ Method 3 failed. Trying Method 4..."
        
        # Method 4: Using pip install directly via Airflow command
        echo "METHOD 4: Using pip install via Airflow command"
        AIRFLOW_CMD="import sys, subprocess; subprocess.check_call([sys.executable, '-m', 'pip', 'install', '$CUSTOM_PACKAGE_NAME @ $TEMP_GCS_PATH'])"
        if gcloud composer environments run "$COMPOSER_ENV_NAME" \
            --location "$COMPOSER_LOCATION" \
            python -- -c "$AIRFLOW_CMD"; then
            echo "✅ SUCCESS: Installed package via direct pip command in Airflow."
        else
            echo "❌ Method 4 failed. Trying Method 5..."
            
            # Method 5: Last resort - create pure Python installer
            echo "METHOD 5: Creating a pure Python installer file"
            INSTALLER_PATH="installer.py"
            cat > "$INSTALLER_PATH" <<EOL
import os
import sys
import subprocess
import tempfile
import shutil
from google.cloud import storage

# Download the wheel from GCS
client = storage.Client()
bucket_name = "${GCS_BUCKET#gs://}"
blob_name = "${TEMP_GCS_PATH#$GCS_BUCKET/}"
temp_dir = tempfile.mkdtemp()
local_file = os.path.join(temp_dir, "${WHEEL_FILENAME}")

print(f"Downloading {blob_name} from {bucket_name} to {local_file}")
bucket = client.bucket(bucket_name)
blob = bucket.blob(blob_name)
blob.download_to_filename(local_file)

# Install the wheel
print(f"Installing wheel file: {local_file}")
subprocess.check_call([sys.executable, "-m", "pip", "install", local_file])

# Clean up
shutil.rmtree(temp_dir)
print("Installation complete")
EOL
            
            echo "Uploading installer.py to GCS..."
            gsutil cp "$INSTALLER_PATH" "${GCS_BUCKET}/temp/installer.py"
            
            echo "Running installer.py in Airflow environment..."
            if gcloud composer environments run "$COMPOSER_ENV_NAME" \
                --location "$COMPOSER_LOCATION" \
                python -- -c "exec(open('/home/airflow/gcs/temp/installer.py').read())"; then
                echo "✅ SUCCESS: Package installed via custom installer script."
            else
                echo "❌ All automatic installation methods failed."
                echo ""
                echo "MANUAL INSTALLATION INSTRUCTIONS:"
                echo "1. Go to Composer Environment > Environment Configuration > PyPI Packages"
                echo "2. Click 'Edit' and add the package with this EXACT format:"
                echo "   $CUSTOM_PACKAGE_NAME @ $TEMP_GCS_PATH"
                echo ""
                echo "Alternative manual installation (GCP Console):"
                echo "1. Navigate to your Composer environment in GCP Console"
                echo "2. Go to PyPI Packages tab and click EDIT"
                echo "3. Add a custom package with this path: $TEMP_GCS_PATH"
                echo ""
                echo "Alternative via gcloud (copy and run this command):"
                echo "gcloud composer environments update $COMPOSER_ENV_NAME --location $COMPOSER_LOCATION --update-pypi-package=\"$CUSTOM_PACKAGE_NAME @ $TEMP_GCS_PATH\""
                echo ""
                echo "Wheel file locations:"
                echo "  - Original GCS path: $WHEEL_GCS_PATH"
                echo "  - Clean GCS path: $TEMP_GCS_PATH"
            fi
        fi
    fi
fi

# --- Step 6: Set Airflow Variables in Composer ---
echo_section_header "Step 6: Setting Airflow Variables in Composer (from .env file or defaults)"

# Variables needed by book_data_ingestion_dag.py
# These should be defined in your .env file or they will use defaults here.
# The DAG itself also has defaults if variables are not found in Airflow.
GCP_PROJECT_ID_VAR=${GCP_PROJECT_ID:-$(gcloud config get-value project)}
COMPOSER_LOCATION_VAR=${COMPOSER_LOCATION} # This is the region of composer itself, which is usually the CF region too
NYT_FUNCTION_NAME_VAR=${NYT_INGEST_FUNCTION_NAME:-nyt-ingest-http} # Default if not in .env
OL_FUNCTION_NAME_VAR=${OPEN_LIBRARY_INGEST_FUNCTION_NAME:-open-library-ingest-http} # Default if not in .env
GB_FUNCTION_NAME_VAR=${GOOGLE_BOOKS_INGEST_FUNCTION_NAME:-google-books-ingest-http} # Default if not in .env

AIRFLOW_VARIABLES_TO_SET=(
    "gcp_project_id:$GCP_PROJECT_ID_VAR"
    "gcp_location:$COMPOSER_LOCATION_VAR"
    "nyt_ingest_function_name:$NYT_FUNCTION_NAME_VAR"
    "open_library_ingest_function_name:$OL_FUNCTION_NAME_VAR"
    "google_books_ingest_function_name:$GB_FUNCTION_NAME_VAR"
)

ALL_VARS_PRESENT=true
for var_entry in "${AIRFLOW_VARIABLES_TO_SET[@]}"; do
    KEY=$(echo "$var_entry" | cut -d: -f1)
    VALUE=$(echo "$var_entry" | cut -d: -f2-)
    if [ -z "$VALUE" ]; then # Check if value is empty string after sourcing .env and applying defaults
        echo "WARNING: Value for Airflow variable '$KEY' is effectively empty. It might not be set correctly in .env or script defaults."
        # ALL_VARS_PRESENT=false # Decide if this is critical enough to skip all
    fi
done

echo "Setting/Updating Airflow variables in Composer environment '$COMPOSER_ENV_NAME'..."
for var_entry in "${AIRFLOW_VARIABLES_TO_SET[@]}"; do
    KEY=$(echo "$var_entry" | cut -d: -f1)
    VALUE=$(echo "$var_entry" | cut -d: -f2-)
    echo "Setting Airflow variable: $KEY to '$VALUE'"
    if ! gcloud composer environments run "$COMPOSER_ENV_NAME" \
        --location "$COMPOSER_LOCATION" variables set -- "$KEY" "$VALUE"; then
        echo "ERROR: Failed to set Airflow variable $KEY. Please set it manually via the Airflow UI."
        # Consider whether to exit or continue
    fi
done
echo "Airflow variable setting process complete."


# --- Final Instructions ---
echo_section_header "Deployment Script Finished!"