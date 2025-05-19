from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook
from google.cloud import storage
from google.api_core.exceptions import NotFound
import logging
import json
import os
import pandas as pd


from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class StorageClient(ABC):

    @abstractmethod
    def _get_client_connection(self):
        pass

    @abstractmethod
    def _get_client(self):
        pass

    @abstractmethod
    def store_data(self):
        pass


class MinioClient(StorageClient):
    
    airflow_connection_id = 'minio'

    def __init__(self):
        self.connection = self._get_client_connection()
        self.client = self._get_client()


    def _get_client_connection(self):
        """
        Retrieve MinIO connection credentials from Airflow connections.
        """
        connection = BaseHook.get_connection(self.airflow_connection_id)
        logger.info(f"Minio connection is retrieved: {connection.extra_dejson['endpoint_url']}")
        return connection
    
    def _get_client(self):
        """
        Create and return a MinIO client.
        """
        extra = self.connection.extra_dejson
        if 'endpoint_url' not in extra:
            raise ValueError("Missing 'endpoint_url' in Airflow MinIO connection.")

        client = Minio(
            endpoint=extra['endpoint_url'].split('//')[1],
            access_key=self.connection.login,
            secret_key=self.connection.password,
            secure=False
        )

        try:
            buckets = client.list_buckets()
            logger.info("MinIO Client Built Successfully!")
            logger.info(f"Available Buckets:, {[bucket.name for bucket in buckets]}")
        except Exception as e:
            raise Exception("MinIO Client Failed:", str(e))

        return client
    
    
    def store_data(self, data, bucket_name, object_name):

        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)
            logging.info(f"Bucket created: {bucket_name}")
        else:
            logging.info(f"{bucket_name}: Bucket exists")

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=BytesIO(data),
            length=len(data)
,
        )

        storage_path = f"s3://{bucket_name}/{object_name.rsplit('/', 1)[0]}"

        return storage_path 


class GCSClient(StorageClient):

    airflow_connection_id = 'google_cloud_default'

    def __init__(self):
        self.connection = self._get_client_connection()
        self.client = self._get_client()
        

    def _get_client_connection(self):
        """
        Retrieve GCP connection credentials from Airflow.
        """
        connection = BaseHook.get_connection(self.airflow_connection_id)
        extra = connection.extra_dejson
        if 'project' not in extra:
            raise ValueError("Missing GCP project in Airflow connection extras.")

        logger.info("GCP connection is retrieved successfully.")

        extra = connection.extra_dejson
        keyfile_dict = json.loads(extra.get('keyfile_dict'))

        if not keyfile_dict:
            raise ValueError("GCP service account key is not configured properly in Airflow.")

        credentials_path = "/tmp/gcp_key.json"
        with open(credentials_path, "w") as f:
            json.dump(keyfile_dict, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

        return connection
    

    def _get_client(self):
        """
        Create and return a GCS client using the service account key.
        """

        client = storage.Client()
        logger.info("Google Cloud Storage Client Initialized Successfully.")
        return client
    

    def store_data(self, data, bucket_name, object_name):
        """
        Store binary data in GCS.
        """
        try:
            bucket = self.client.get_bucket(bucket_name)
            #logger.info(f"Bucket exists: {bucket_name}")

        except NotFound:
            bucket = self.client.bucket(bucket_name)
            self.client.create_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' not found. Creating...")


        blob = bucket.blob(object_name)

        if object_name.endswith('.json'):
            data = data.encode('utf-8')
            blob.upload_from_file(BytesIO(data), content_type="application/json", rewind=True)

        elif object_name.endswith('.csv'):
            data = data.encode('utf-8')
            blob.upload_from_file(BytesIO(data), content_type="text/csv", rewind=True)


        return f"gs://{bucket_name}/{object_name}"
    

    def read_raw_json_to_df(self, bucket_name, raw_prefix):

        bucket = self.client.bucket(bucket_name)

        # List all JSON files under raw/
        blobs = bucket.list_blobs(prefix=raw_prefix)

        all_dfs = []

        for blob in blobs:
            blob_name = blob.name
            if blob_name.endswith(".json"):
                logger.info(f"Processing: {blob_name}")

                #Read and parse JSON
                json_bytes = blob.download_as_bytes()
                json_str = json_bytes.decode("utf-8")
                try:
                    json_data = json.loads(json_str)
                    df = pd.json_normalize(json_data)
                    all_dfs.append(df)
                    logger.info(f"Loaded: {blob_name}")
                except json.JSONDecodeError:
                    logger.info(f"Skipping {blob_name}: invalid JSON")
                    continue


        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
        
        else:
            final_df = pd.DataFrame()

        return final_df

    

