import requests

import pandas as pd
from helper.store_to_gcs import store_json_to_gcs

bucket='767-abinash'
prefix='raw'
file_name='weather-openweather'

url = "https://api.openweathermap.org/data/2.5/weather?lat=38.989697&lon=-76.937759&appid=6990ef80c0c1164af794cc4e340985fa"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

data = response.json() 
store_json_to_gcs(data, bucket, prefix, file_name)

#print(data)
#weather_info = {
#    "city": data.get("name"),
#    "country": data.get("sys", {}).get("country"),
#    "latitude": data.get("coord", {}).get("lat"),
#    "longitude": data.get("coord", {}).get("lon"),
#    "weather_main": data.get("weather", [{}])[0].get("main"),
#    "weather_description": data.get("weather", [{}])[0].get("description"),
#    "temperature": data.get("main", {}).get("temp"),
#    "humidity": data.get("main", {}).get("humidity"),
#    "pressure": data.get("main", {}).get("pressure"),
#    "wind_speed": data.get("wind", {}).get("speed"),
#    "timestamp": pd.to_datetime(data.get("dt"), unit="s")
#}

#df = pd.DataFrame([weather_info])

#df.to_csv("raw/weather.csv")

#print(df)
