import os
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta


load_dotenv()

#NYC MTA API for DATA
url = "https://data.ny.gov/resource/wujg-7c2s.json"

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    #print(data)
    with open('john_davitz/metro.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)
else:
    print(f"Error: {response.status_code}")


#US Weather data for manhattan 2020-2024
url = "https://archive-api.open-meteo.com/v1/archive?latitude=40.7685&longitude=-73.9822&start_date=2025-05-01&end_date=2025-05-02&hourly=temperature_2m,rain,precipitation,weather_code&timezone=America%2FNew_York&temperature_unit=fahrenheit&wind_speed_unit=mph"

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    #print(data)
    with open('john_davitz/weather.json', 'w') as json_file:
        json.dump(data, json_file, indent=4)
else:
    print(f"Error: {response.status_code}")


#Tomtom traffic data

tomtom_key = os.getenv("TOMTOM_KEY")

body = {
    "jobName": "Test job",
    "distanceUnit": "KILOMETERS",
    "mapType": "GENESIS",
    "mapVersion": "2023.12",
    "acceptMode": "AUTO",
    "network": {
        "name": "test",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [
                        [19.44305,51.75612],
                        [19.44992,51.75612],
                        [19.44992,51.75947],
                        [19.44305,51.75947],
                        [19.44305,51.75612]
                    ]
                ]
            ]
        },
        "timeZoneId": "Europe/NewYork",
        "frcs": [
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7"
        ],
        "probeSource": "ALL"
    },
    "dateRange": {
        "name": "October 2024",
        "from": "2024-08-01",
        "to": "2024-08-31"
    },
    "timeSets": [
        {
            "name": "Friday morning hour",
            "timeGroups": [
                {
                    "days": [
                        "FRI"
                    ],
                    "times": [
                        "7:00-8:00"
                    ]
                }
            ]
        }
    ]
}

url = "https://fargoprod.blob.core.windows.net/fargo-prod/jobs%2F6194937%2Fresults%2FManhattan.json?sv=2025-01-05&se=2027-05-12T18%3A18%3A07Z&sr=b&sp=r&sig=POpW51bvswX5TN7bc1d06ey0uCXS1rVMrykG4mQVhMQ%3D"
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    print(len(data["network"]['segmentResults']))

else:
    print(f"Error: {response.status_code}")

# Define start and end dates
start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 12, 31)

# every day in loop
tomtom_jobs = []
current_date = start_date
while current_date <= end_date:
    current = current_date.strftime("%Y-%m-%d")  # Format as YYYY-MM-DD
    next = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")
    print(current)
    print(next)

    url = f"https://api.tomtom.com/traffic/trafficstats/areaanalysis/1?key={tomtom_key}"

    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "jobName": "Manhattan",
        "distanceUnit": "MILES",
        "mapType": "GENESIS",
        "mapVersion": "2023.12",
        "acceptMode": "AUTO",
        "network": {
            "name": "test",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-74.0119628390001,40.74698412534],
                        [-74.0207362330436,40.7031712634046],
                        [-74.0119942661048,40.6985482431613],
                        [-73.9957844432873,40.7072962465332],
                        [-73.9770936974176,40.709577439774],
                        [-73.9716249812543,40.7189106247328],
                        [-73.9713677911817,40.7366518757552],
                        [-73.9641861480767,40.7494130178921],
                        [-73.9431951255015,40.77368938442],
                        [-73.9283618307868,40.7939183664455],
                        [-73.9317306056171,40.8083118536605],
                        [-73.9334675475727,40.8337276712066],
                        [-73.9305969102116,40.8427211423688],
                        [-73.9221812687955,40.8554506970262],
                        [-73.9094089232874,40.8715921542751],
                        [-73.9258155292187,40.8786371353669],
                        [-74.0119628390001,40.74698412534] 
                    ]
                ]
            },
            "timeZoneId": "America/New_York",
            "frcs": [
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6"
            ],
            "probeSource": "ALL"
        },
        "dateRange": {
            "name": "current auto",
            "from": str(current),
            "to": str(next)
            #"from": "2024-08-06",
            #"to": "2024-08-06"
        },
        "timeSets": [
            {
                "name": "Friday morning hour",
                "timeGroups": [
                    {
                        "days": [
                            "MON","TUE","WED","THU","FRI","SAT","SUN"
                        ],
                        "times": [
                            "00:00-24:00"
                        ]
                    }
                ]
            }
        ]
    }


    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print(data)
        print(data['jobId'])
        #with open('john_davitz/weather.json', 'w') as json_file:
        #    json.dump(data, json_file, indent=4)
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        print(response.json()['jobId'] + ". Already STarted")
    
    break



    current_date += timedelta(days=1)  # Move to the next day