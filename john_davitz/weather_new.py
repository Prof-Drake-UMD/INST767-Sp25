import pandas as pd
# main.py
import requests
from google.cloud import storage

def fetch_and_upload(request):

    # Step 1: Fetch from API
    #US Weather data for manhattan 2020-2024
    url = "https://archive-api.open-meteo.com/v1/archive?latitude=40.7685&longitude=-73.9822&start_date=2020-01-01&end_date=2025-05-02&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,weather_code,precipitation_sum&timezone=America%2FNew_York&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch"

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        #print(data)

    else:
        return(f"Error: {response.status_code}")

    weather_df = pd.DataFrame(data['daily'])


    weather_df = weather_df.rename(columns={"time": "date", "temperature_2m_mean": "mean_temp","temperature_2m_max": "max_temp", "temperature_2m_min": "min_temp" })

    #weather_df.sort_values(by='date').to_csv('john_davitz/weather.csv')
    print(weather_df.sort_values(by='date'))

    # Step 2: Upload to Cloud Storage
    client = storage.Client()
    bucket = client.bucket('api_output_bucket_inst_final')
    blob = bucket.blob('output/weather_api_output.csv')
    #blob.upload_from_string(weather_df.to_csv(), content_type='text/csv')
    csv_data = weather_df.to_csv(index=False).encode('utf-8')
    blob.upload_from_string(csv_data)

    return 'Upload complete \n'

