

#!/bin/bash
echo "⚠️  This script is deprecated and has been replaced by deploy_pubsub_pipeline.sh"
exit 0

#!/bin/bash
# Movie Data Cloud Function Deployment Script (Now Playing only)


ENV_FILE="google_cloud/.env"
set -a
source "$ENV_FILE"
set +a

# Create requirements.txt if not exists
REQUIREMENTS_FILE="google_cloud/requirements.txt"
if [ ! -f "$REQUIREMENTS_FILE" ]; then
  echo "Creating requirements.txt..."
  cat > "$REQUIREMENTS_FILE" <<EOL
requests
python-dotenv
google-cloud-storage
EOL
else
  echo "requirements.txt already exists."
fi

# Load .env variables into shell environment
source <(grep = $ENV_FILE)

# Configuration
PROJECT_ID="${PROJECT_ID}"
BUCKET_NAME="movie-data-bucket-$PROJECT_ID"
FUNCTION_NAME="fetch_now_playing_movies"
REGION="us-central1"

echo "🚀 Starting deployment..."

# Set gcloud project
gcloud config set project "$PROJECT_ID"

# Enable necessary services
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com storage.googleapis.com

# Create GCS bucket if it doesn't already exist
if ! gsutil ls -b gs://$BUCKET_NAME >/dev/null 2>&1; then
  echo "Creating bucket: $BUCKET_NAME"
  gsutil mb -l $REGION gs://$BUCKET_NAME
else
  echo "Bucket already exists: $BUCKET_NAME"
fi

# Read .env into ENV_VARS for --set-env-vars
ENV_VARS=$(grep -v '^#' $ENV_FILE | xargs | sed 's/ /,/g')
echo "Parsed environment variables: $ENV_VARS"

# Deploy the Cloud Function (1st Gen)
gcloud functions deploy "$FUNCTION_NAME" \
  --runtime python39 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point=main_entry \
  --region="$REGION" \
  --source=google_cloud \
  --memory=256MB \
  --timeout=540s \
  --set-env-vars="GCS_BUCKET=$BUCKET_NAME,$ENV_VARS" \
  --no-gen2

echo "✅ Deployment complete!"
echo "🪣 Bucket: gs://$BUCKET_NAME"
