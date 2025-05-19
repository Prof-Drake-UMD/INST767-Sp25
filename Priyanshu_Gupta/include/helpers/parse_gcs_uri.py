import re

def extract_parts(gcs_uri):
    # Use regex to extract parts
    pattern = r'gs://([^/]+)/([^/]+)/raw/(\d{4}/\d{2}/\d{2})/(.+)'
    match = re.match(pattern, gcs_uri)
    
    if match:
        bucket_name = match.group(1)
        table_prefix = match.group(2)
        partition = match.group(3)
        file_name = match.group(4)
        return bucket_name, table_prefix, partition
    else:
        raise ValueError("Invalid GCS URI format")

# gcs_uri = "gs://openfda-drug-events/drugs-adverse-events/raw/2024/01/02"
# bucket_name, prefix, date = extract_parts(gcs_uri)

# print("Bucket Name:", bucket_name)
# print("Prefix:", prefix)
# print("Date:", date)
