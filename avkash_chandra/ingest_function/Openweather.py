import requests
import json

API_KEY = '6210981ad6654fbf2aa6bcfcea9dc4d6'

def get_weather_data(city):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=imperial'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        return {
            'city': data['name'],
            'temperature_F': data['main']['temp'],
            'weather': data['weather'][0]['main'],
            'description': data['weather'][0]['description']
        }
    else:
        print(f"Weather API error for '{city}':", response.status_code)
        return {
            'city': city,
            'temperature_F': 'N/A',
            'weather': 'N/A',
            'description': 'N/A'
        }

# 👇 Optional test block
if __name__ == "__main__":
    sample_city = "College Park"
    weather_info = get_weather_data(sample_city)

    with open("weather_data.json", "w") as f:
        json.dump(weather_info, f, indent=4)

    print(f"Saved weather data for '{sample_city}':", weather_info)

