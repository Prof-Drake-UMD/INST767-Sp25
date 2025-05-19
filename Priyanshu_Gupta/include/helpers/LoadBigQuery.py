from google.cloud import bigquery
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class LoadBigQuery():
    
    def __init__(self, bucket, blob_path, bq_table):
        self.bucket=bucket
        self.blob_path=blob_path
        self.bq_table=bq_table
        self.gcs_uri = f"gs://{self.bucket}/{self.blob_path}"
        self.client=self._get_client()
    
    def _get_client(self):
        """
        Create and return a BigQuery client.
        """
        return bigquery.Client()
    
    
    def load_into_bq(self, partition_date=None):

        # # Set variables
        
        if partition_date:
            table_id = f"{self.bq_table}${partition_date}"
            
        else:
            table_id = f"{self.bq_table}"

        # Configure the load job
        if self.blob_path.endswith('.json'):
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition="WRITE_TRUNCATE",  # or "WRITE_TRUNCATE" to overwrite
                autodetect=True  # Let BigQuery detect schema from the Parquet file
            )

        elif self.blob_path.endswith('.csv'):

            job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            field_delimiter=",",
            encoding="UTF-8"
            )

        else:
            raise ValueError(f"Unsupported file format for: {self.blob_path}")

        # Load the json file to GCS
        load_job = self.client.load_table_from_uri(
            self.gcs_uri,
            table_id,
            job_config=job_config
        )
        

        # Wait for the job to complete
        load_job.result()

        logger.info(f"Loadin data from {self.gcs_uri} for partition {partition_date} into BigQuery table {table_id}")

        


    