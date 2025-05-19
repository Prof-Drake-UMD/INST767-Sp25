import base64
import json
from api_logic import flatten_and_export

def main(event, context):
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
