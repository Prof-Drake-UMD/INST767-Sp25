import base64
import json
import pandas as pd
import io
from google.cloud import storage
import os
from google.cloud import pubsub_v1

def fetch_and_upload(event, context):
    print("signal recieved - running")
    