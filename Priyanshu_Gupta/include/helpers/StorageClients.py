from io import BytesIO
from minio import Minio
from airflow.hooks.base import BaseHook
import logging


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
        

