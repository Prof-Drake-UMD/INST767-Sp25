import csv
from api_calls import get_weather_data

def transform_weather():
    w = get_weather_data("College Park", "cc39e55645488541724622e038d6a2c3")
    with open('output/weather_logs.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'city', 'temperature', 'humidity', 'weather_description', 'wind_speed', 'cloud_coverage', 'logged_at'])
        writer.writerow([
            f"{w['id']}_{w['dt']}", w['name'], w['main']['temp'], w['main']['humidity'],
            w['weather'][0]['description'], w['wind']['speed'], w['clouds']['all'],
            w['dt']
        ])

