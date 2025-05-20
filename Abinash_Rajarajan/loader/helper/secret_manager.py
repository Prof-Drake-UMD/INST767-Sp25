import os
from google.cloud import secretmanager

def get_secret(secret_id: str) -> str:
    project = os.environ["GCP_PROJECT"]
    name    = f"projects/{project}/secrets/{secret_id}/versions/latest"
    client  = secretmanager.SecretManagerServiceClient()
    payload = client.access_secret_version(request={"name": name})\
                    .payload.data
    return payload.decode("utf-8")