from helper.load_from_gcs import load_json_from_gcs
from helper.store_to_gcs import store_df_to_gcs
import pandas as pd



# Extract events list
bucket='767-abinash'
raw_prefix='raw'
file_name='events-tickermaster'

file_path = f'{raw_prefix}/{file_name}.json'

transfrom_prefix='transform'


events_json = load_json_from_gcs(bucket, file_path)

# events_json = events_json.get('_embedded', {})

event = events_json['_embedded']['events'][0]
#print(event['name'])

# print(events_json)
# Extract required fields from events
event_data = []
for event in events_json.get('_embedded', {}).get('events', []):
    event_info = {
        'name': event.get('name'),
        'postalCode': event.get('_embedded', {}).get('venues', [{}])[0].get('postalCode'),
        'startDateTime': event.get('dates', {}).get('start', {}).get('dateTime'),
        'endDateTime': event.get('dates', {}).get('end', {}).get('dateTime')  # May be None
    }
    event_data.append(event_info)

# Convert the extracted data to a Pandas DataFrame
events_df = pd.DataFrame(event_data)
events_df['startDateTime'] = pd.to_datetime(events_df['startDateTime'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
events_df['endDateTime'] = pd.to_datetime(events_df['endDateTime'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')

store_df_to_gcs(events_df, bucket, transfrom_prefix, file_name)


'''
# Prepare list of dicts for each event
event_data = []
for event in events_json['_embedded']['events'][0]:
    event_info = {
        'name': event.get('name'),
        'postalCode': event.get('_embedded', {}).get('venues', [{}])[0].get('postalCode'),
        'startDateTime': event.get('dates', {}).get('start', {}).get('dateTime'),
        'endDateTime': event.get('dates', {}).get('end', {}).get('dateTime')  # May be None
    }
    event_data.append(event_info)

# Create DataFrame
df = pd.DataFrame(event_data)



# df.to_csv("raw/events.csv")

# # Display the DataFrame
# #print(df)

'''







