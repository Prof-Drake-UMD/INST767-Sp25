import requests

import pandas as pd

from helper.store_to_gcs import store_json_to_gcs

bucket='767-abinash'
prefix='raw'
file_name='events-tickermaster'


url = "https://app.ticketmaster.com/discovery/v2/events.json?size=100&apikey=2dSO4F20UzPUplsMi3yA8spZlpmccz4K"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

data = response.json() 

store_json_to_gcs(data, bucket, prefix, file_name)

print(data)
#