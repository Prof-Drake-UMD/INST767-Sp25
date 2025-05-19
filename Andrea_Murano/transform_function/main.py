import os
import sys
import base64
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from api_logic import flatten_and_export

def main(event, context):
    """
    Google Cloud Function entry point.
    Triggered by Pub/Sub when ingest function publishes a message.
    Receives data, then flattens and exports it (e.g., to CSV).
    """
    try:
        raw_data = base64.b64decode(event['data']).decode('utf-8')
        result = json.loads(raw_data)
    except Exception as e:
        print(f"Error decoding Pub/Sub message: {e}")
        return

    try:
        flatten_and_export(result)
        print("Flattened and exported result.")
    except Exception as e:
        print(f"Error flattening/exporting: {e}")
