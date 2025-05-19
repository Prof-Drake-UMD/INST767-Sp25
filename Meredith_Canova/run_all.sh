#!/usr/bin/env bash
# Trigger the three ingest Cloud Functions in parallel and

REGION=${1:-us-central1}     

echo "Triggering ingest functions in $REGION …"

for FN in ingest_stocks ingest_news ingest_trends; do
  URL=$(gcloud functions describe "$FN" --region="$REGION" \
        --format='value(serviceConfig.uri)')
  echo "POST $URL"
  curl -s -X POST "$URL" &
done
wait
echo "✓ all ingests triggered"


echo; echo "Tailing cleaner / loader logs (Ctrl‑C to exit)…"
timeout 60s bash -c "
  gcloud functions logs read cleaner --region=$REGION --gen2 --tail &
  gcloud functions logs read loader  --region=$REGION --gen2 --tail &
  wait
"
echo "Log tail ended."
