#!/usr/bin/env bash
set -euo pipefail

# ── 0) Load config ─────────────────────────────────────────────────────────────
ENV_FILE="env.yaml"
TMP_ENV=$(mktemp)
cp "$ENV_FILE" "$TMP_ENV"

# Extract values from env.yaml
RAW_BUCKET=$(grep '^RAW_BUCKET:' "$ENV_FILE" | cut -d ':' -f2 | tr -d ' "')
CLEAN_BUCKET=$(grep '^CLEAN_BUCKET:' "$ENV_FILE" | cut -d ':' -f2 | tr -d ' "')
BQ_DATASET=$(grep '^BQ_DATASET:' "$ENV_FILE" | cut -d ':' -f2 | tr -d ' "')
PROJECT_ID=$(grep '^GCP_PROJECT_ID:' "$ENV_FILE" | cut -d ':' -f2 | tr -d ' "')
REGION="us-central1"

# Issue 4 
# ── 1) Show context ────────────────────────────────────────────────────────────
echo "Deploying Market-Pulse pipeline to project $PROJECT_ID in $REGION"

# ── 2) Enable required GCP APIs ───────────────────────────────────────────────
gcloud config set project "$PROJECT_ID"
gcloud services enable \
  cloudfunctions.googleapis.com \
  pubsub.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com

# ── 3) Create GCS buckets if needed ───────────────────────────────────────────
gsutil ls -b "gs://${RAW_BUCKET}" >/dev/null 2>&1 || \
  gsutil mb -l "$REGION" "gs://${RAW_BUCKET}"
gsutil ls -b "gs://${CLEAN_BUCKET}" >/dev/null 2>&1 || \
  gsutil mb -l "$REGION" "gs://${CLEAN_BUCKET}"

# ── 4) Create Pub/Sub topics if needed ────────────────────────────────────────
for TOPIC in market-transform market-load; do
  gcloud pubsub topics describe "$TOPIC" >/dev/null 2>&1 || \
    gcloud pubsub topics create "$TOPIC"
done

# ── 5) Create BigQuery dataset if needed ──────────────────────────────────────
bq --project_id="$PROJECT_ID" ls "$BQ_DATASET" >/dev/null 2>&1 || \
  bq --location="$REGION" mk --dataset "${PROJECT_ID}:${BQ_DATASET}"

# ── 6) Apply table schemas using SQL file ─────────────────────────────────────
echo "Applying BigQuery schema from sql/create_tables.sql..."
if bq --project_id="$PROJECT_ID" query \
     --use_legacy_sql=false \
     --location="$REGION" \
     < sql/create_tables.sql; then
  echo "BigQuery schema applied successfully."
else
  echo "Failed to apply BigQuery schema."
  exit 5
fi



# ── 7) Deploy HTTP ingest functions ------------------------------------------
echo "Deploying ingest_stocks …"
gcloud functions deploy ingest_stocks \
  --gen2 \
  --project="$PROJECT_ID" --region="$REGION" \
  --runtime=python312 \
  --source=src/ingest/stocks \
  --entry-point=ingest_stocks \
  --trigger-http --allow-unauthenticated \
  --env-vars-file="$TMP_ENV" \
  --memory=256Mi --timeout=640s

echo "Deploying ingest_news …"
gcloud functions deploy ingest_news \
  --gen2 \
  --project="$PROJECT_ID" --region="$REGION" \
  --runtime=python312 \
  --source=src/ingest/news \
  --entry-point=ingest_news \
  --trigger-http \
  --allow-unauthenticated \
  --env-vars-file="$TMP_ENV" \
  --memory=512Mi --timeout=240s

echo "Deploying ingest_trends …"
gcloud functions deploy ingest_trends \
  --gen2 \
  --project="$PROJECT_ID" --region="$REGION" \
  --runtime=python312 \
  --source=src/ingest/trends \
  --entry-point=ingest_trends \
  --trigger-http --allow-unauthenticated \
  --env-vars-file="$TMP_ENV" \
  --memory=256Mi --timeout=180s

# ── 8) Deploy cleaner (Pub/Sub) ──────────────────────────────────────────────
echo "Deploying cleaner..."
gcloud functions deploy cleaner \
  --gen2 \
  --project="$PROJECT_ID" --region="$REGION" \
  --runtime=python312 \
  --entry-point=handle_raw_message \
  --trigger-topic=market-transform \
  --source=src/transform \
  --env-vars-file="$TMP_ENV" \
  --memory=512Mi --timeout=120s

# ── 9) Deploy loader (Pub/Sub) ───────────────────────────────────────────────
echo "Deploying loader..."
gcloud functions deploy loader \
  --gen2 \
  --project="$PROJECT_ID" --region="$REGION" \
  --runtime=python312 \
  --entry-point=handle_json_message \
  --trigger-topic=market-load \
  --source=src/load \
  --env-vars-file="$TMP_ENV" \
  --memory=512Mi --timeout=120s

# ── 7) Done – show how to trigger the three ingests ---------------------------
echo -e "\nDeployment complete. Kick off the pipeline with:"
for FN in ingest_stocks ingest_news ingest_trends; do
  URL=$(gcloud functions describe "$FN" --region="$REGION" \
        --format='value(serviceConfig.uri)')
  echo "  curl -X POST $URL"
done

# ── 11) Clean up ──────────────────────────────────────────────────────────────
rm -f "$TMP_ENV"
echo "Deployment script finished."