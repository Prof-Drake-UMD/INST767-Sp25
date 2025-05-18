import os
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
import gc

# API config
url = "https://data.ny.gov/resource/wujg-7c2s.json"
limit = 10000000
max_rows = 110696370
nyc_token = ""

# Threaded fetcher
def fetch_and_process(offset):
    this_url = f"{url}?$limit={limit}&$offset={offset}&$order=:id&$$app_token={nyc_token}&$where=transit_timestamp > '2023-01-01T00:00:00'"
    print(f"Fetching: offset {offset}")
    try:
        response = requests.get(this_url)
        if response.status_code == 200:
            json_data = response.json()
            temp_df = pd.DataFrame(json_data)

            if temp_df.empty:
                return pd.DataFrame()

            temp_df = temp_df[['transit_timestamp', 'transit_mode', 'borough', 'payment_method', 'ridership']]
            temp_df = temp_df[temp_df['borough'].str.lower() == 'manhattan']
            temp_df['transit_timestamp'] = pd.to_datetime(temp_df['transit_timestamp'])
            temp_df = temp_df[temp_df['transit_timestamp'] >= '2020-01-01']
            print(f"Complete: {offset/max_rows}")
            return temp_df
        else:
            print(f"Error at offset {offset}: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Exception at offset {offset}: {e}")
        return pd.DataFrame()

# Use threads to fetch in parallel
offsets = list(range(0, max_rows, limit))
metro_df = pd.DataFrame()

with ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(fetch_and_process, offset) for offset in offsets]
    for future in as_completed(futures):
        metro_df = pd.concat([metro_df, future.result()])
        print(f"Length of df is {len(metro_df)}")

# Process & upload
metro_df['date'] = metro_df['transit_timestamp'].dt.date
metro_df['ridership'] = metro_df['ridership'].astype(float)
result = metro_df.groupby('date', as_index=False)['ridership'].sum()

# Upload to GCS
client = storage.Client()
bucket = client.bucket('api_output_bucket_inst_final')
blob = bucket.blob('output/metro_api_output.csv')
blob.upload_from_string(result.to_csv(index=False), content_type='text/csv')

print("Done.")
