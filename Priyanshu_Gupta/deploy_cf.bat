@echo off
SET FUNCTION_NAME=transform_drug_data
SET RUNTIME=python310
SET TRIGGER_TOPIC=airflow-raw-event
SET ENTRY_POINT=main
SET SOURCE_DIR=cloudFunctionTransform
SET MEMORY=1024MB
SET REGION=us-central1
SET PROJECT_ID=inst767-openfda-pg

gcloud functions deploy %FUNCTION_NAME% ^
  --runtime %RUNTIME% ^
  --trigger-topic %TRIGGER_TOPIC% ^
  --entry-point %ENTRY_POINT% ^
  --source=%SOURCE_DIR% ^
  --memory=%MEMORY% ^
  --region=%REGION% ^
  --project=%PROJECT_ID%
