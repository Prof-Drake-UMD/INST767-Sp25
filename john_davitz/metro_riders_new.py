import os
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import time
# main.py
import requests
from google.cloud import storage


#vehicle count data
url = "https://data.ny.gov/resource/wujg-7c2s.json"
limit = 1000000
max_rows = 110696370 #110,696,370
offset = 0
metro_df = pd.DataFrame()
nyc_token = os.getenv("TRAFFIC_KEY")
while offset < max_rows:
    this_url = url + f"?$limit={limit}&$offset={offset}&$order=:id&$$app_token={nyc_token}&$where=transit_timestamp > '2023-01-01T00:00:00'"
    print(f"Complete: {offset/max_rows} {this_url}")
    print("{:,}".format(offset))
    response = requests.get(this_url)
    if response.status_code == 200:
        json_data = response.json()
        temp_df = pd.DataFrame(json_data)
        print(f"Temp Length {len(temp_df)}")
        #drop extra cols
        temp_df = temp_df[['transit_timestamp', 'transit_mode', 'borough', 'payment_method', 'ridership']]
        #filter only manhatan
        temp_df = temp_df[temp_df['borough'].str.lower() == 'manhattan']
        #filter out before 2020
        temp_df['transit_timestamp'] = pd.to_datetime(temp_df['transit_timestamp'])
        temp_df = temp_df[temp_df['transit_timestamp'] >= '2020-01-01']
        print(f"Temp Length after cutoff {len(temp_df)}")
        metro_df = pd.concat([metro_df, temp_df])
        
    else:
        print(f"Error: {response.status_code}")
    offset = offset + limit
    print(f"Total len {len(metro_df)}")
    time.sleep(1)
metro_df['date'] = metro_df['transit_timestamp'].dt.date
metro_df['time'] = metro_df['transit_timestamp'].dt.time
metro_df['ridership'] = metro_df['ridership'].astype(float)
metro_df = metro_df.groupby('date', as_index=False)['ridership'].sum()


# Upload to Cloud Storage
client = storage.Client()
bucket = client.bucket('api_output_bucket_inst_final')
blob = bucket.blob('output/metro_api_output.csv')
#blob.upload_from_string(weather_df.to_csv(), content_type='text/csv')
csv_data = metro_df.to_csv(index=False).encode('utf-8')
blob.upload_from_string(csv_data)
print('\n done \n')