import base64
import json
import logging
import os
from datetime import datetime, timezone
import uuid
from google.cloud import bigquery
import time

logger = logging.getLogger(__name__)

bq_client_global = None

def get_bigquery_client():
    global bq_client_global
    if bq_client_global is None:
        logger.info("Initializing BigQuery client.")
        bq_client_global = bigquery.Client()
    return bq_client_global

class MessageHandler:
    def __init__(self, event_id):
        self.event_id = event_id

    def decode_and_parse(self, cloud_event_data):
        if not cloud_event_data or not cloud_event_data.get("message"):
            logger.error(f"[Event ID: {self.event_id}] No Pub/Sub message data received.")
            return None

        message_data_encoded = cloud_event_data["message"].get("data")
        if not message_data_encoded:
            logger.error(f"[Event ID: {self.event_id}] No data in Pub/Sub message.")
            return None

        message_data_str = "" 
        try:
            message_data_str = base64.b64decode(message_data_encoded).decode("utf-8")
            data = json.loads(message_data_str)
            raw_count = len(data) if isinstance(data, (list, dict)) else 1
            logger.info(f"[Event ID: {self.event_id}] Successfully decoded and parsed JSON. Received {raw_count} raw entries/structure.")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"[Event ID: {self.event_id}] Could not decode or parse JSON from Pub/Sub message: {e}. Message content (first 100 chars): '{message_data_str[:100]}'")
            return None
        except Exception as e: 
            logger.error(f"[Event ID: {self.event_id}] General error decoding Pub/Sub message: {e}", exc_info=True)
            return None

class BaseTransformer:
    def __init__(self, event_id):
        self.event_id = event_id
        self.current_ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def transform_data(self, raw_data_list):
        if not isinstance(raw_data_list, list):
            logger.error(f"[Event ID: {self.event_id}] Expected a list of items for transformation, but got {type(raw_data_list)}")
            return [], 0 

        if not raw_data_list:
            logger.info(f"[Event ID: {self.event_id}] Received an empty list of items to transform.")
            return [], 0

        transformed_items = []
        skipped_count = 0
        
        logger.info(f"[Event ID: {self.event_id}] Starting transformation for {len(raw_data_list)} items...")
        for index, item_data in enumerate(raw_data_list):
            try:
                if not isinstance(item_data, dict):
                    logger.warning(f"[Event ID: {self.event_id}] Item at index {index} is not a dictionary. Skipping.")
                    skipped_count += 1
                    continue
                
                transformed_item = self._transform_item(item_data, index)
                if transformed_item:
                    transformed_items.append(transformed_item)
                else:
                    skipped_count += 1 
            except Exception as e:
                logger.error(f"[Event ID: {self.event_id}] Error transforming item at index {index}: {e}", exc_info=True)
                skipped_count += 1
        
        logger.info(f"[Event ID: {self.event_id}] Transformation complete. Transformed {len(transformed_items)} items. Skipped {skipped_count} items.")
        return transformed_items, skipped_count

    def _transform_item(self, item_data, index):
        """Transforms a single item. Subclasses must implement this method.
        Should return the transformed item dictionary, or None if the item is to be skipped.
        If returning None, the method is responsible for logging the skip reason.
        """
        raise NotImplementedError("Subclasses must implement _transform_item method.")

class BigQueryLoader:
    def __init__(self, event_id, table_id):
        self.event_id = event_id
        self.table_id = table_id
        self.client = get_bigquery_client()

    def load_data(self, data_to_load):
        if not data_to_load: 
            logger.info(f"[Event ID: {self.event_id}] No data provided to load into BigQuery table {self.table_id}.")
            return True 

        logger.info(f"[Event ID: {self.event_id}] Preparing to load {len(data_to_load)} rows into BigQuery table {self.table_id}.")
        try:
            errors = self.client.insert_rows_json(self.table_id, data_to_load)
            if not errors:
                logger.info(f"[Event ID: {self.event_id}] Successfully loaded {len(data_to_load)} rows into {self.table_id}")
                return True
            else:
                logger.error(f"[Event ID: {self.event_id}] Encountered errors while inserting {len(data_to_load)} rows into {self.table_id}. Number of rows with errors: {len(errors)}.")
                for error_detail in errors: 
                    logger.error(f"  [Event ID: {self.event_id}] Row index: {error_detail.get('index')}, Errors:")
                    for err in error_detail.get('errors', []): 
                        logger.error(f"    Reason: {err.get('reason')}, Location: {err.get('location')}, Message: {err.get('message')}")
                return False
        except Exception as e:
            logger.error(f"[Event ID: {self.event_id}] CRITICAL ERROR: Failed to insert rows into BigQuery table {self.table_id} due to an exception: {e}", exc_info=True)
            return False 

    def merge_data(self, 
                   rows: list[dict[str, any]], 
                   merge_keys: list[str],
                   update_columns: list[str] | None = None,
                   insert_columns: list[str] | None = None,
                   staging_dataset_id: str | None = None,
                   max_retries: int = 3,
                   retry_delay: int = 2
                  ):
        """Merges data into a BigQuery table using a staging table.

        Args:
            rows: A list of dictionaries to be merged.
            merge_keys: A list of column names that define uniqueness for the MERGE operation.
            update_columns: A list of columns that should be updated if a match is found. 
                            If None, all columns in 'rows' not in 'merge_keys' are updated.
            insert_columns: A list of columns to insert for new rows. 
                            If None, all columns from 'rows' are used.
            staging_dataset_id: Optional dataset ID for the staging table. Defaults to the target table's dataset.
            max_retries: Maximum number of retries for concurrent update errors.
            retry_delay: Delay in seconds between retries.

        Returns:
            True if the merge operation was successful (or no data to merge), False otherwise.
        """
        if not rows:
            logger.info(f"[Event ID: {self.event_id}] No data provided to merge into BigQuery table {self.table_id}.")
            return True

        if not merge_keys:
            logger.error(f"[Event ID: {self.event_id}] Merge keys must be provided for MERGE operation on table {self.table_id}.")
            return False

        logger.info(f"[Event ID: {self.event_id}] Starting MERGE operation for {len(rows)} rows into {self.table_id}.")

        # Determine staging table details
        target_table_obj = self.client.get_table(self.table_id)
        target_table_ref = target_table_obj.reference
        
        actual_staging_dataset_id = staging_dataset_id if staging_dataset_id else target_table_ref.dataset_id
        random_str = str(uuid.uuid4())[:8]
        staging_table_name = f"staging_{target_table_ref.table_id}_{random_str}"
        staging_table_id_str = f"{target_table_ref.project}.{actual_staging_dataset_id}.{staging_table_name}"
        staging_table_schema = target_table_obj.schema

        logger.info(f"[Event ID: {self.event_id}] Staging table will be: {staging_table_id_str}")

        job_config = bigquery.LoadJobConfig()
        job_config.schema = staging_table_schema

        try:
            logger.info(f"[Event ID: {self.event_id}] Creating and loading staging table: {staging_table_id_str}")
            load_job = self.client.load_table_from_json(rows, staging_table_id_str, job_config=job_config)
            load_job.result()
            logger.info(f"[Event ID: {self.event_id}] Staging table {staging_table_id_str} created and loaded with {len(rows)} rows.")

            all_row_columns = list(rows[0].keys()) if rows else []
            effective_insert_columns = insert_columns if insert_columns is not None else all_row_columns
            if not effective_insert_columns:
                logger.error(f"[Event ID: {self.event_id}] No columns specified or inferred for INSERT part of MERGE.")
                return False

            effective_update_columns = []
            if update_columns is None:
                effective_update_columns = [col for col in all_row_columns if col not in merge_keys]
            else:
                effective_update_columns = update_columns

            on_conditions = " AND ".join([f"T.`{key}` IS NOT DISTINCT FROM S.`{key}`" for key in merge_keys])
            
            update_set_clauses = ""
            if effective_update_columns:
                update_set_clauses = "UPDATE SET " + ", ".join([f"T.`{col}` = S.`{col}`" for col in effective_update_columns])
            
            insert_col_names = ", ".join([f"`{col}`" for col in effective_insert_columns])
            insert_val_names = ", ".join([f"S.`{col}`" for col in effective_insert_columns])

            merge_sql = f"""
                MERGE INTO `{self.table_id}` T
                USING `{staging_table_id_str}` S
                ON {on_conditions}
            """
            if update_set_clauses:
                merge_sql += f"\n    WHEN MATCHED THEN {update_set_clauses}"
            
            merge_sql += f"""
                WHEN NOT MATCHED BY TARGET THEN
                  INSERT ({insert_col_names})
                  VALUES ({insert_val_names})
            """

            logger.info(f"[Event ID: {self.event_id}] Executing MERGE statement:\n{merge_sql}")
            
            # Add retry logic for concurrent update errors
            retry_count = 0
            while retry_count < max_retries:
                try:
                    merge_job = self.client.query(merge_sql)
                    merge_job.result()
                    logger.info(f"[Event ID: {self.event_id}] MERGE operation completed successfully for table {self.table_id}.")
                    return True
                except Exception as e:
                    if "concurrent update" in str(e).lower() and retry_count < max_retries - 1:
                        retry_count += 1
                        wait_time = retry_delay * (2 ** retry_count)  # Exponential backoff
                        logger.warning(f"[Event ID: {self.event_id}] Concurrent update detected. Retrying in {wait_time} seconds (attempt {retry_count + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    raise  # Re-raise if not a concurrent update error or out of retries

            return False

        except Exception as e:
            logger.error(f"[Event ID: {self.event_id}] Error during MERGE operation for table {self.table_id}: {e}", exc_info=True)
            return False
        finally:
            try:
                logger.info(f"[Event ID: {self.event_id}] Deleting staging table: {staging_table_id_str}")
                self.client.delete_table(staging_table_id_str, not_found_ok=True)
                logger.info(f"[Event ID: {self.event_id}] Staging table {staging_table_id_str} deleted.")
            except Exception as e:
                logger.error(f"[Event ID: {self.event_id}] Error deleting staging table {staging_table_id_str}: {e}", exc_info=True)