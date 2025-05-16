#!/bin/bash

# Config
PROJECT_ID="mystic-primacy-459515-e8"
REGION="us-central1"
PROJECT_NUMBER="454433469605"
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
TIME_ZONE="America/New_York"

echo "Creating USDA scheduler..."
gcloud scheduler jobs create http usda-job \
  --schedule="0 9 * * *" \
  --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/usda-publisher" \
  --http-method=GET \
  --oidc-service-account-email="$SERVICE_ACCOUNT" \
  --project="$PROJECT_ID" \
  --time-zone="$TIME_ZONE" \
  --location="$REGION"

echo "Creating Nutritionix scheduler..."
gcloud scheduler jobs create http nutritionix-job \
  --schedule="30 9 * * *" \
  --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/nutritionix-publisher" \
  --http-method=GET \
  --oidc-service-account-email="$SERVICE_ACCOUNT" \
  --project="$PROJECT_ID" \
  --time-zone="$TIME_ZONE" \
  --location="$REGION"

echo "Creating Wger scheduler..."
gcloud scheduler jobs create http wger-job \
  --schedule="0 10 * * *" \
  --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/wger-publisher" \
  --http-method=GET \
  --oidc-service-account-email="$SERVICE_ACCOUNT" \
  --project="$PROJECT_ID" \
  --time-zone="$TIME_ZONE" \
  --location="$REGION"

echo "All scheduler jobs created successfully."
