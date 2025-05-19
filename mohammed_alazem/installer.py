import os
import sys
import subprocess
import tempfile
import shutil
from google.cloud import storage

# Download the wheel from GCS
client = storage.Client()
bucket_name = "us-central1-book-processing-e6a205d4-bucket"
blob_name = "temp/book_pipeline_consumer_func-0.1.0-py3-none-any.whl"
temp_dir = tempfile.mkdtemp()
local_file = os.path.join(temp_dir, "book_pipeline_consumer_func-0.1.0-py3-none-any.whl")

print(f"Downloading {blob_name} from {bucket_name} to {local_file}")
bucket = client.bucket(bucket_name)
blob = bucket.blob(blob_name)
blob.download_to_filename(local_file)

# Install the wheel
print(f"Installing wheel file: {local_file}")
subprocess.check_call([sys.executable, "-m", "pip", "install", local_file])

# Clean up
shutil.rmtree(temp_dir)
print("Installation complete")
