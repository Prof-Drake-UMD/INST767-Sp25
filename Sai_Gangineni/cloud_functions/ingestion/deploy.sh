#!/bin/bash

# ----------------------------- CONFIG -----------------------------
# Replace these with your actual credentials
NUTRITIONIX_APP_ID=e7b7f0e3
NUTRITIONIX_APP_KEY=8b5d276a9e6acb387913c6f10d716ca3
USDA_API_KEY=ZwiliNrJ5HBTMcnkPl2lbCkJq1fkS1BBKCjM9CQg
REGION=us-central1

# ----------------------------- SUBSCRIBERS -----------------------------
echo "Deploying Nutritionix Subscriber..."
gcloud functions deploy nutritionix-subscriber \
  --runtime python310 \
  --entry-point nutritionix_subscriber \
  --trigger-topic nutrition-topic \
  --set-env-vars NUTRITIONIX_APP_ID=$NUTRITIONIX_APP_ID,NUTRITIONIX_APP_KEY=$NUTRITIONIX_APP_KEY,\
  --region=$REGION

echo "Deploying USDA Subscriber..."
gcloud functions deploy usda-subscriber \
  --runtime python310 \
  --entry-point usda_subscriber \
  --trigger-topic usda-topic \
  --set-env-vars USDA_API_KEY=$USDA_API_KEY,\
  --region=$REGION

echo "Deploying Wger Subscriber..."
gcloud functions deploy wger-subscriber \
  --runtime python310 \
  --entry-point wger_subscriber \
  --trigger-topic wger-topic \
  --region=$REGION

# ----------------------------- PUBLISHERS -----------------------------
echo "Deploying Nutritionix Publisher..."
gcloud functions deploy nutritionix-publisher \
  --runtime python310 \
  --entry-point nutritionix_publisher \
  --trigger-http \
  --allow-unauthenticated \
  --region=$REGION

echo "Deploying USDA Publisher..."
gcloud functions deploy usda-publisher \
  --runtime python310 \
  --entry-point usda_publisher \
  --trigger-http \
  --allow-unauthenticated \
  --region=$REGION

echo "Deploying Wger Publisher..."
gcloud functions deploy wger-publisher \
  --runtime python310 \
  --entry-point wger_publisher \
  --trigger-http \
  --allow-unauthenticated \
  --region=$REGION

echo "All functions deployed successfully."
