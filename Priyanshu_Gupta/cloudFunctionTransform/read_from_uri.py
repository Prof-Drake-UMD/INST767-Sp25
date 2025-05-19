from urllib.parse import urlparse
import pandas as pd
import json

from google.cloud import storage

storage_client = storage.Client()


def read_from_uri(gcs_uri):
        """
        Reads a JSON file from GCS and returns its content as a Python DF.

        Args:
            gcs_uri (str): The GCS URI, e.g. 'gs://bucket_name/path/to/file.json'

        Returns:
            dict or list: Parsed JSON content
        """
        parsed = urlparse(gcs_uri)
        bucket_name = parsed.netloc
        blob_path = parsed.path.lstrip("/")

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        data = blob.download_as_bytes()
        json_data = json.loads(data.decode("utf-8"))

        df = pd.json_normalize(json_data)

        return df
