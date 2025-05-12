import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import time

load_dotenv()

#vehicle count data
url = "https://data.cityofnewyork.us/resource/btm5-ppia.json"
#      https://data.cityofnewyork.us/resource/btm5-ppia.json
limit = 10000
max_rows = 45000
offset = 0
traffic_df = pd.DataFrame()
nyc_token = os.getenv("TRAFFIC_KEY")

while offset < max_rows:
    print(offset)
    this_url = url + f"?$limit={limit}&$offset={offset}&$order=:id&$$app_token={nyc_token}"
    print(this_url)
    response = requests.get(this_url)
    if response.status_code == 200:
        json_data = response.json()
        
        temp_columns = ['id', 'segmentid', 'roadway_name', 'from', 'to', 'direction', 'date','_12_00_1_00_am', '_1_00_2_00am', '_2_00_3_00am', '_3_00_4_00am', '_4_00_5_00am', '_5_00_6_00am', '_6_00_7_00am', '_7_00_8_00am', '_8_00_9_00am', '_9_00_10_00am', '_10_00_11_00am', '_11_00_12_00pm','_12_00_1_00pm', '_1_00_2_00pm', '_2_00_3_00pm', '_3_00_4_00pm', '_4_00_5_00pm', '_5_00_6_00pm', '_6_00_7_00pm', '_7_00_8_00pm', '_8_00_9_00pm', '_9_00_10_00pm', '_10_00_11_00pm', '_11_00_12_00am']


        temp_df = pd.DataFrame(json_data, columns=temp_columns)

        print(f"Temp Length {len(temp_df)}")

        #drop pre 2020 data 
        temp_df['date'] = pd.to_datetime(temp_df['date'])
        # Define your cutoff date
        cutoff_date = pd.to_datetime('2020-01-01')

        # Keep only dates after the cutoff date
        temp_df = temp_df[temp_df['date'] > cutoff_date]

        print(f"Temp Length after cutoff {len(temp_df)}")
        
        traffic_df = pd.concat([traffic_df, temp_df])
        
        #print(data)
        #with open('john_davitz/vehicle_traffic.json', 'w') as json_file:
        #    json.dump(data, json_file, indent=4)
    else:
        print(f"Error: {response.status_code}")
    offset = offset + limit
    print(f"Total len {len(traffic_df)}")
    time.sleep(1)

print(len(traffic_df))

traffic_df['date'] = pd.to_datetime(traffic_df['date'])

#cast int
cols_to_cast = ['_12_00_1_00_am', '_1_00_2_00am', '_2_00_3_00am', '_3_00_4_00am', '_4_00_5_00am', '_5_00_6_00am', '_6_00_7_00am', '_7_00_8_00am', '_8_00_9_00am', '_9_00_10_00am', '_10_00_11_00am', '_11_00_12_00pm', '_12_00_1_00pm', '_1_00_2_00pm', '_2_00_3_00pm', '_3_00_4_00pm', '_4_00_5_00pm', '_5_00_6_00pm', '_6_00_7_00pm', '_7_00_8_00pm', '_8_00_9_00pm', '_9_00_10_00pm', '_10_00_11_00pm', '_11_00_12_00am']

traffic_df[cols_to_cast] = traffic_df[cols_to_cast].astype(int)

traffic_df['daily_sum'] = traffic_df[['_12_00_1_00_am', '_1_00_2_00am', '_2_00_3_00am', '_3_00_4_00am', '_4_00_5_00am', '_5_00_6_00am', '_6_00_7_00am', '_7_00_8_00am', '_8_00_9_00am', '_9_00_10_00am', '_10_00_11_00am', '_11_00_12_00pm', '_12_00_1_00pm', '_1_00_2_00pm', '_2_00_3_00pm', '_3_00_4_00pm', '_4_00_5_00pm', '_5_00_6_00pm', '_6_00_7_00pm', '_7_00_8_00pm', '_8_00_9_00pm', '_9_00_10_00pm', '_10_00_11_00pm', '_11_00_12_00am']].sum(axis=1)

traffic_df = traffic_df[['id','segmentid','roadway_name','from','to','direction','date','daily_sum']]

traffic_df = traffic_df.groupby('date', as_index=False)['daily_sum'].sum()

traffic_df.sort_values(by='date').to_csv('john_davitz/traffic.csv')

print(traffic_df.dtypes)