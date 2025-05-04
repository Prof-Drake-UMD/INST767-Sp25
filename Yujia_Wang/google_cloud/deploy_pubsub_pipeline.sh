#!/bin/bash
# Deployment script for Pub/Sub-based movie data pipeline

# --- CONFIGURATION ---
ENV_FILE="google_cloud/.env"

if [ ! -f "$ENV_FILE" ]; then
  echo "❌ .env file not found at: $ENV_FILE"
  exit 1
fi

# Load environment variables from .env
set -o allexport
source "$ENV_FILE"
set +o allexport

if [ -z "$PROJECT_ID" ]; then
  echo "❌ PROJECT_ID is not set in .env"
  exit 1
fi

gcloud config set project "$PROJECT_ID"

gcloud services enable \
  cloudfunctions.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com \
  artifactregistry.googleapis.com \
  pubsub.googleapis.com \
  storage.googleapis.com

# --- VARIABLES ---
BUCKET_NAME="movie-data-bucket-$PROJECT_ID"
REGION="us-central1"
TOPIC_NAME="now_playing_topic"
SUB_NAME="now_playing_sub"
INGEST_FUNCTION_NAME="fetch_now_playing_movies"
TRANSFORM_FUNCTION_NAME="transform_movie_data"

echo "🚀 Starting deployment..."
echo "📌 PROJECT_ID = $PROJECT_ID"
echo "📌 REGION = $REGION"
echo "📌 BUCKET_NAME = $BUCKET_NAME"

# --- CREATE PUBSUB TOPIC & SUBSCRIPTION ---
gcloud pubsub topics create "$TOPIC_NAME" --quiet || echo "✅ Topic already exists"
gcloud pubsub subscriptions create "$SUB_NAME" --topic="$TOPIC_NAME" --quiet || echo "✅ Subscription already exists"

# --- CREATE BUCKET (if not exists) ---
if ! gsutil ls -b "gs://$BUCKET_NAME" > /dev/null 2>&1; then
    gsutil mb -l "$REGION" "gs://$BUCKET_NAME"
    echo "✅ Bucket created: $BUCKET_NAME"
else
    echo "ℹ️  Bucket already exists: $BUCKET_NAME"
fi

# --- PARSE ENV VARS ---
ENV_VARS=$(grep -v '^#' "$ENV_FILE" | grep -v '^$' | paste -sd, -)
echo "🔐 Environment variables parsed: $ENV_VARS"

# --- DEPLOY INGEST FUNCTION ---
echo "🚀 Deploying ingest function: $INGEST_FUNCTION_NAME"
gcloud functions deploy "$INGEST_FUNCTION_NAME" \
  --runtime python39 \
  --trigger-http \
  --allow-unauthenticated \
  --entry-point=main_entry \
  --region="$REGION" \
  --source=google_cloud/ingest \
  --memory=256MB \
  --timeout=540s \
  --set-env-vars="$ENV_VARS,TOPIC_NAME=$TOPIC_NAME,GCS_BUCKET=$BUCKET_NAME" \
  --no-gen2

# --- DEPLOY TRANSFORM FUNCTION ---
echo "🚀 Deploying transform function: $TRANSFORM_FUNCTION_NAME"
gcloud functions deploy "$TRANSFORM_FUNCTION_NAME" \
  --runtime python39 \
  --trigger-topic="$TOPIC_NAME" \
  --entry-point=main_entry \
  --region="$REGION" \
  --source=google_cloud/transform \
  --memory=256MB \
  --timeout=540s \
  --set-env-vars="$ENV_VARS,GCS_BUCKET=$BUCKET_NAME"\
  --gen2

echo "✅ All functions deployed successfully!"
echo "🪣 GCS Bucket: gs://$BUCKET_NAME"
echo "📩 Pub/Sub Topic: $TOPIC_NAME"
echo "🔁 Subscription: $SUB_NAME"

LAST_SUCCESS_PATH="google_cloud/last_success.json"

if [ -f "$LAST_SUCCESS_PATH" ]; then
  echo "📤 Uploading fallback last_success.json to GCS as last_success..."
  gsutil cp "$LAST_SUCCESS_PATH" gs://movie-data-bucket-$PROJECT_ID/metadata/last_success.json || echo "⚠️ Upload failed"
else
  echo "❌ $LAST_SUCCESS_PATH not found"
fi

echo "✅ Deployment complete!"
echo "🚀 You can now trigger the ingest function via HTTP or set up a schedule in Cloud Scheduler."