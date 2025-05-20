import requests

import pandas as pd

from helper.store_to_gcs import store_json_to_gcs

bucket='767-abinash'
prefix='raw'
file_name='yelp_businesses'

url = "https://api.yelp.com/v3/businesses/search?location=college%20park&sort_by=best_match&limit=50"

headers = {
    "accept": "application/json",
    "authorization": "Bearer kPJyhtbXNht0dKW17uYpDXEGN3-o64AmQpHH7wszlxQWwAfS02jAvDqfoPxZ7No1x5P3idDGfDf09aINlBr-RZMmwb6N2Cq3E7YHWc-o5qCcof-ZJwEN0MJC2lciaHYx"
}

response = requests.get(url, headers=headers)

data = response.json()

store_json_to_gcs(data, bucket, prefix, file_name)



# df = pd.DataFrame([data])

# df.to_csv("raw/weather.csv")

#print(data)