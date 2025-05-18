import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import time
import requests
from google.cloud import storage

load_dotenv()

def fetch_and_upload(request):

    #vehicle count data
    url = "https://data.cityofnewyork.us/resource/7ym2-wayt.json"
    #      https://data.cityofnewyork.us/resource/btm5-ppia.json
    limit = 100000
    max_rows = 1712605
    offset = 0
    traffic_df = pd.DataFrame()
    nyc_token = os.getenv("TRAFFIC_KEY")
    print(f"API Key is {nyc_token}")


    while offset < max_rows:
        this_url = url + f"?$limit={limit}&$offset={offset}&$order=:id&$$app_token={nyc_token}"
        print(this_url)
        response = requests.get(this_url)
        if response.status_code == 200:
            json_data = response.json()
            temp_df = pd.DataFrame(json_data)

            #drop pre 2020 data 
            temp_df['yr'] = temp_df['yr'].astype(int)
            temp_df = temp_df[temp_df['yr'] >= 2020]

            #filter only manhatan
            temp_df = temp_df[temp_df['boro'].str.lower() == 'manhattan']

            print(f"Temp Length after cutoff {len(temp_df)}")
            traffic_df = pd.concat([traffic_df, temp_df])
            
        else:
            print(f"Error: {response.status_code}")
        offset = offset + limit
        print(f"Total len {len(traffic_df)}")
        time.sleep(1)

    traffic_df['date'] = traffic_df['yr'].astype(str) + "-" + traffic_df['m'].astype(str) + "-" + traffic_df['d'].astype(str)
    #df['new_col'] = df['col1'].astype(str) + '-' + df['col2'].astype(str) + '-' + df['col3'].astype(str)
    traffic_df['date'] = pd.to_datetime(traffic_df['date'])

    traffic_df['vol'] = traffic_df['vol'].astype(int)

    traffic_df = traffic_df.groupby('date', as_index=False)['vol'].sum()

    traffic_df = traffic_df.sort_values(by='date') #.to_csv('john_davitz/traffic.csv')

    
    # Upload to Cloud Storage
    client = storage.Client()
    bucket = client.bucket('api_output_bucket_inst_final')
    blob = bucket.blob('output/traffic.csv')
    #blob.upload_from_string(weather_df.to_csv(), content_type='text/csv')
    csv_data = traffic_df.to_csv(index=False).encode('utf-8')
    blob.upload_from_string(csv_data)

    return 'Upload complete \n'