#!/bin/bash

# Exit on any error - no point continuing if something breaks
set -e

echo "----------------------------------------------------------------------"
echo "--- Setting up GCP resources for the book data pipeline ---"
echo "----------------------------------------------------------------------"

# Load up our environment variables
if [ -f .env ]; then
  echo "Loading up environment variables from .env..."
  set -o allexport
  source .env
  set +o allexport
  echo "✅ Environment variables loaded"
else
  echo "⚠️  No .env file found - make sure you have one with your API keys and project settings"
  echo "Continuing with whatever's set in the environment..."
fi

# Default values for our config - override these in .env
GCP_PROJECT_ID=${GCP_PROJECT_ID:-"default-project-id-please-set-in-env"}
GCP_REGION=${GCP_REGION:-"us-central1"}
NYT_API_KEY=${NYT_API_KEY:-"NYT_API_KEY_PLACEHOLDER"}
GOOGLE_BOOKS_API_KEY=${GOOGLE_BOOKS_API_KEY:-"GOOGLE_BOOKS_API_KEY_PLACEHOLDER"}

# Pub/Sub topics for each data source
NYT_PUBSUB_TOPIC_NAME=${NYT_PUBSUB_TOPIC_NAME:-"nyt-books-raw-data"}
OPEN_LIBRARY_PUBSUB_TOPIC_NAME=${OPEN_LIBRARY_PUBSUB_TOPIC_NAME:-"open-library-raw-data"}
GOOGLE_BOOKS_PUBSUB_TOPIC_NAME=${GOOGLE_BOOKS_PUBSUB_TOPIC_NAME:-"google-books-raw-data"}

# BigQuery settings
BIGQUERY_DATASET_NAME=${BIGQUERY_DATASET_NAME:-"mohammed_alazem_dataset"}
PYTHON_RUNTIME=${PYTHON_RUNTIME:-"python311"}

# Service accounts we'll be using
AIRFLOW_SA_EMAIL="pipeline-setup@realtime-book-data-gcp.iam.gserviceaccount.com"
COMPOSER_ENV_SA_EMAIL="realtime-book-data-gcp@appspot.gserviceaccount.com"

echo ""
echo "--- Here's what we're working with ---"
echo "GCP Project ID:              $GCP_PROJECT_ID"
echo "GCP Region:                  $GCP_REGION"
echo "NYT API Key:                 ${NYT_API_KEY:0:5}********"
echo "Google Books API Key:        ${GOOGLE_BOOKS_API_KEY:0:5}********"
echo "NYT Pub/Sub Topic:           $NYT_PUBSUB_TOPIC_NAME"
echo "Open Library Pub/Sub Topic:  $OPEN_LIBRARY_PUBSUB_TOPIC_NAME"
echo "Google Books Pub/Sub Topic:  $GOOGLE_BOOKS_PUBSUB_TOPIC_NAME"
echo "BigQuery Dataset Name:       $BIGQUERY_DATASET_NAME"
echo "Cloud Function Runtime:      $PYTHON_RUNTIME"
echo "Airflow Service Account:     $AIRFLOW_SA_EMAIL"
echo "Composer Env Service Account: $COMPOSER_ENV_SA_EMAIL"
echo "-----------------------------"
echo ""

# Make sure we have the essential stuff set up
if [ "$GCP_PROJECT_ID" == "default-project-id-please-set-in-env" ] || [ "$GCP_PROJECT_ID" == "your-gcp-project-id" ] || [ "$GCP_PROJECT_ID" == "inst767-book-pipeline" ]; then
  echo "❌ Oops! Your GCP project ID isn't set properly"
  echo "Please set it in your .env file"
  exit 1
fi
if [ "$NYT_API_KEY" == "NYT_API_KEY_PLACEHOLDER" ]; then
  echo "❌ Missing NYT API key - add it to your .env file"
  exit 1
fi
if [ "$GOOGLE_BOOKS_API_KEY" == "GOOGLE_BOOKS_API_KEY_PLACEHOLDER" ]; then
  echo "❌ Missing Google Books API key - add it to your .env file"
  exit 1
fi
echo "✅ All the essential config looks good!"
echo ""

# Set up gcloud CLI and get our project number
echo "--- Setting up gcloud CLI and getting project info ---"
echo "Setting project to $GCP_PROJECT_ID..."
gcloud config set project $GCP_PROJECT_ID
echo "Setting default region to $GCP_REGION..."
gcloud config set functions/region $GCP_REGION
gcloud config set run/region $GCP_REGION

PROJECT_NUMBER=$(gcloud projects describe $GCP_PROJECT_ID --format='value(projectNumber)')
if [ -z "$PROJECT_NUMBER" ]; then
    echo "❌ Couldn't get project number - check your project ID and permissions"
    exit 1
fi
DEFAULT_COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
echo "Project number: $PROJECT_NUMBER"
echo "Default compute SA: $DEFAULT_COMPUTE_SA"
echo "✅ gcloud CLI is ready to go"
echo ""

# Enable the APIs we need
echo "--- Enabling GCP APIs ---"
SERVICES_TO_ENABLE=(
  "cloudfunctions.googleapis.com"
  "pubsub.googleapis.com"
  "bigquery.googleapis.com"
  "cloudbuild.googleapis.com"
  "iam.googleapis.com"
  "eventarc.googleapis.com"
  "artifactregistry.googleapis.com"
  "run.googleapis.com"
  "cloudresourcemanager.googleapis.com"
)
echo "Enabling: ${SERVICES_TO_ENABLE[*]}"
gcloud services enable "${SERVICES_TO_ENABLE[@]}" --project=$GCP_PROJECT_ID
echo "✅ APIs enabled"
echo ""

# Create our Pub/Sub topics
echo "--- Creating Pub/Sub topics ---"
PUBSUB_TOPICS=($NYT_PUBSUB_TOPIC_NAME $OPEN_LIBRARY_PUBSUB_TOPIC_NAME $GOOGLE_BOOKS_PUBSUB_TOPIC_NAME)
for topic in "${PUBSUB_TOPICS[@]}"; do
  echo "Setting up topic: $topic..."
  if gcloud pubsub topics describe $topic --project=$GCP_PROJECT_ID >/dev/null 2>&1; then
    echo "Topic $topic already exists"
  else
    gcloud pubsub topics create $topic --project=$GCP_PROJECT_ID
    echo "Created topic $topic"
  fi
done
echo "✅ Pub/Sub topics ready"
echo ""

# Create our BigQuery dataset
echo "--- Creating BigQuery dataset ---"
echo "Setting up dataset: $BIGQUERY_DATASET_NAME in $GCP_REGION..."
if bq --project_id=$GCP_PROJECT_ID --location=$GCP_REGION ls --datasets | grep -qw $BIGQUERY_DATASET_NAME; then
    echo "Dataset $BIGQUERY_DATASET_NAME already exists"
else
    bq --location=$GCP_REGION mk --dataset \
        --default_table_expiration 0 \
        --description "Dataset for Book Data Pipeline project" \
        $GCP_PROJECT_ID:$BIGQUERY_DATASET_NAME
    echo "Created dataset $BIGQUERY_DATASET_NAME"
fi
echo "✅ BigQuery dataset ready"
echo ""

# Create our BigQuery tables
echo "--- Creating BigQuery tables ---"
SQL_SCHEMA_FILES=("./sql/nyt_books_schema.sql" "./sql/open_library_books_schema.sql" "./sql/google_books_schema.sql")
for schema_file in "${SQL_SCHEMA_FILES[@]}"; do
  if [ -f "$schema_file" ]; then
    echo "Processing schema: $schema_file..."
    bq query --project_id=$GCP_PROJECT_ID --dataset_id=$BIGQUERY_DATASET_NAME --use_legacy_sql=false < "$schema_file"
    echo "Created tables from $schema_file"
  else
    echo "⚠️  Schema file $schema_file not found - skipping"
  fi
done
echo "✅ BigQuery tables ready"
echo ""

# Set up IAM roles for our service accounts
echo "--- Setting up IAM roles for Cloud Functions ---"
CF_RUNTIME_ROLES_TO_GRANT=(
  "roles/pubsub.publisher"
  "roles/pubsub.subscriber"
  "roles/bigquery.dataEditor"
  "roles/bigquery.jobUser"
  "roles/artifactregistry.writer"
)
for role in "${CF_RUNTIME_ROLES_TO_GRANT[@]}"; do
  echo "Granting $role to $AIRFLOW_SA_EMAIL..."
  if gcloud projects get-iam-policy $GCP_PROJECT_ID --flatten="bindings[].members" --format='value(bindings.role)' \
     --filter="bindings.members:serviceAccount:$AIRFLOW_SA_EMAIL AND bindings.role:$role" | grep -q "$role"; then
      echo "Role $role already granted"
  else
      gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
          --member="serviceAccount:$AIRFLOW_SA_EMAIL" \
          --role="$role" --condition=None
      echo "Granted $role"
  fi
done
echo "✅ IAM roles set up for Cloud Functions"
echo ""

# Set up Composer SA permissions
echo "--- Setting up Composer SA permissions ---"
COMPOSER_SA_DISCOVERY_ROLES=(
  "roles/cloudfunctions.viewer"
)
for role in "${COMPOSER_SA_DISCOVERY_ROLES[@]}"; do
  echo "Granting $role to $COMPOSER_ENV_SA_EMAIL..."
  if gcloud projects get-iam-policy $GCP_PROJECT_ID --flatten="bindings[].members" --format='value(bindings.role)' \
     --filter="bindings.members:serviceAccount:$COMPOSER_ENV_SA_EMAIL AND bindings.role:$role" | grep -q "$role"; then
      echo "Role $role already granted"
  else
      gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
          --member="serviceAccount:$COMPOSER_ENV_SA_EMAIL" \
          --role="$role" --condition=None
      echo "Granted $role"
  fi
done
echo "✅ Composer SA permissions set up"
echo ""

# Deploy our Cloud Functions
echo "--- Deploying Cloud Functions ---"
echo "Using Python $PYTHON_RUNTIME"
echo "Running as service account: $AIRFLOW_SA_EMAIL"
echo "HTTP functions will be public (no auth required)"

# Deploy NYT function
echo "Deploying NYT function..."
gcloud functions deploy nyt-ingest-http \
  --gen2 \
  --runtime=$PYTHON_RUNTIME \
  --entry-point=nyt_http_and_pubsub_trigger \
  --source=. \
  --trigger-http \
  --allow-unauthenticated \
  --service-account=$AIRFLOW_SA_EMAIL \
  --memory=512MiB \
  --set-env-vars=GCP_PROJECT_ID=$GCP_PROJECT_ID,NYT_API_KEY=$NYT_API_KEY,NYT_PUBSUB_TOPIC_NAME=$NYT_PUBSUB_TOPIC_NAME \
  --project=$GCP_PROJECT_ID \
  --region=$GCP_REGION
echo "✅ NYT function deployed"

# Deploy Open Library function
echo "Deploying Open Library function..."
gcloud functions deploy open-library-ingest-http \
  --gen2 \
  --runtime=$PYTHON_RUNTIME \
  --entry-point=open_library_http_and_pubsub_trigger \
  --source=. \
  --trigger-http \
  --allow-unauthenticated \
  --service-account=$AIRFLOW_SA_EMAIL \
  --memory=512MiB \
  --set-env-vars=GCP_PROJECT_ID=$GCP_PROJECT_ID,OPEN_LIBRARY_PUBSUB_TOPIC_NAME=$OPEN_LIBRARY_PUBSUB_TOPIC_NAME \
  --project=$GCP_PROJECT_ID \
  --region=$GCP_REGION
echo "✅ Open Library function deployed"

# Deploy Google Books function
echo "Deploying Google Books function..."
gcloud functions deploy google-books-ingest-http \
  --gen2 \
  --runtime=$PYTHON_RUNTIME \
  --entry-point=google_books_http_and_pubsub_trigger \
  --source=. \
  --trigger-http \
  --allow-unauthenticated \
  --service-account=$AIRFLOW_SA_EMAIL \
  --memory=512MiB \
  --set-env-vars=GCP_PROJECT_ID=$GCP_PROJECT_ID,GOOGLE_BOOKS_API_KEY=$GOOGLE_BOOKS_API_KEY,GOOGLE_BOOKS_PUBSUB_TOPIC_NAME=$GOOGLE_BOOKS_PUBSUB_TOPIC_NAME \
  --project=$GCP_PROJECT_ID \
  --region=$GCP_REGION
echo "✅ Google Books function deployed"

# Deploy subscriber functions
echo "Deploying subscriber functions..."

# NYT subscriber
echo "Deploying NYT subscriber..."
gcloud functions deploy process-nyt-data-subscriber \
  --gen2 \
  --runtime=$PYTHON_RUNTIME \
  --entry-point=process_nyt_data_subscriber \
  --source=. \
  --trigger-topic=$NYT_PUBSUB_TOPIC_NAME \
  --service-account=$AIRFLOW_SA_EMAIL \
  --memory=512MiB \
  --set-env-vars=GCP_PROJECT_ID=$GCP_PROJECT_ID,BIGQUERY_DATASET_NAME=$BIGQUERY_DATASET_NAME \
  --project=$GCP_PROJECT_ID \
  --region=$GCP_REGION
echo "✅ NYT subscriber deployed"

# Open Library subscriber
echo "Deploying Open Library subscriber..."
gcloud functions deploy process-open-library-data-subscriber \
  --gen2 \
  --runtime=$PYTHON_RUNTIME \
  --entry-point=process_open_library_data_subscriber \
  --source=. \
  --trigger-topic=$OPEN_LIBRARY_PUBSUB_TOPIC_NAME \
  --service-account=$AIRFLOW_SA_EMAIL \
  --memory=512MiB \
  --set-env-vars=GCP_PROJECT_ID=$GCP_PROJECT_ID,BIGQUERY_DATASET_NAME=$BIGQUERY_DATASET_NAME \
  --project=$GCP_PROJECT_ID \
  --region=$GCP_REGION
echo "✅ Open Library subscriber deployed"

# Google Books subscriber
echo "Deploying Google Books subscriber..."
gcloud functions deploy process-google-books-data-subscriber \
  --gen2 \
  --runtime=$PYTHON_RUNTIME \
  --entry-point=process_google_books_data_subscriber \
  --source=. \
  --trigger-topic=$GOOGLE_BOOKS_PUBSUB_TOPIC_NAME \
  --service-account=$AIRFLOW_SA_EMAIL \
  --memory=512MiB \
  --set-env-vars=GCP_PROJECT_ID=$GCP_PROJECT_ID,BIGQUERY_DATASET_NAME=$BIGQUERY_DATASET_NAME \
  --project=$GCP_PROJECT_ID \
  --region=$GCP_REGION
echo "✅ Google Books subscriber deployed"

echo ""
echo "----------------------------------------------------------------------"
echo "🎉 All done! Your GCP resources are set up and ready to go."
echo "----------------------------------------------------------------------"