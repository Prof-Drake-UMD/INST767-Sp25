from helper.load_from_gcs import load_json_from_gcs
from helper.store_to_gcs import store_df_to_gcs
import pandas as pd

# Define GCS bucket and file paths
bucket = '767-abinash'
raw_prefix = 'raw'
file_name = 'weather-openweather'

file_path = f'{raw_prefix}/{file_name}.json'
transform_prefix = 'transform'

# Load JSON data from GCS
weather_json = load_json_from_gcs(bucket, file_path)

# Extract required fields from weather data
weather_data = []

# Extract weather information
weather_info = {
    "city": weather_json.get("name"),  # City name
    "country": weather_json.get("sys", {}).get("country"),  # Country code
    "latitude": weather_json.get("coord", {}).get("lat"),  # Latitude
    "longitude": weather_json.get("coord", {}).get("lon"),  # Longitude
    "weather_main": weather_json.get("weather", [{}])[0].get("main"),  # Main weather condition
    "weather_description": weather_json.get("weather", [{}])[0].get("description"),  # Weather description
    "temperature": weather_json.get("main", {}).get("temp"),  # Temperature
    "humidity": weather_json.get("main", {}).get("humidity"),  # Humidity
    "pressure": weather_json.get("main", {}).get("pressure"),  # Pressure
    "wind_speed": weather_json.get("wind", {}).get("speed"),  # Wind speed
    "timestamp": pd.to_datetime(weather_json.get("dt"), unit="s")  # Convert UNIX timestamp to datetime
}

weather_data.append(weather_info)

# Convert the extracted data to a Pandas DataFrame
weather_df = pd.DataFrame(weather_data)

# Save the DataFrame to GCS
store_df_to_gcs(weather_df, bucket, transform_prefix, file_name)
