import requests
import json

# 1️⃣ Fetch Weather Data from OpenWeatherMap
def fetch_weather_data(city, api_key):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return {
            "city": city,
            "timestamp": data.get("dt"),
            "temperature": data["main"].get("temp"),
            "humidity": data["main"].get("humidity"),
            "wind_speed": data["wind"].get("speed"),
            "weather_description": data["weather"][0].get("description") if data.get("weather") else None
        }
    else:
        raise Exception(f"Error {response.status_code}: Unable to fetch weather data")


# 2️⃣ Fetch Air Quality Data from WAQI
def fetch_air_quality_data(city, token):
    url = f"https://api.waqi.info/feed/{city}/?token={token}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["status"] != "ok":
            raise Exception("Invalid WAQI response")
        iaqi = data["data"].get("iaqi", {})
        return {
            "city": city,
            "timestamp": data["data"].get("time", {}).get("s"),
            "aqi": data["data"].get("aqi"),
            "pm25": iaqi.get("pm25", {}).get("v"),
            "pm10": iaqi.get("pm10", {}).get("v"),
            "no2": iaqi.get("no2", {}).get("v"),
            "so2": iaqi.get("so2", {}).get("v"),
            "co": iaqi.get("co", {}).get("v"),
            "o3": iaqi.get("o3", {}).get("v")
        }
    else:
        raise Exception(f"Error {response.status_code}: Unable to fetch air quality data")


# 3️⃣ Fetch Economic Data from World Bank
def fetch_worldbank_data(country_code="IN", indicator="NY.GDP.MKTP.CD"):
    url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{indicator}"
    params = {"format": "json", "per_page": 100}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        json_data = response.json()
        if len(json_data) < 2:
            raise Exception("Unexpected World Bank response format")
        return [
            {
                "country": country_code,
                "indicator": indicator,
                "year": entry.get("date"),
                "value": entry.get("value")
            }
            for entry in json_data[1]
        ]
    else:
        raise Exception(f"Error {response.status_code}: Unable to fetch World Bank data")


# ✅ Test block – only runs if you run this file directly
if __name__ == "__main__":
    OPENWEATHER_API_KEY = "bf32905a0da7fece2559e5c367e5c234"
    WAQI_TOKEN = "2158bce9b080ddecbd37aaf7bdb68ac88e2cbe78"
    CITY = "New York"

    weather = fetch_weather_data(city=CITY, api_key=OPENWEATHER_API_KEY)
    print("🌦️ Weather Data:\n", json.dumps(weather, indent=2))

    air = fetch_air_quality_data(city=CITY, token=WAQI_TOKEN)
    print("🌫️ Air Quality Data:\n", json.dumps(air, indent=2))

    gdp = fetch_worldbank_data(country_code="US", indicator="NY.GDP.MKTP.CD")
    print("💰 GDP Data (Top 5 Years):\n", json.dumps(gdp[:5], indent=2))

    print("\n🧪 Checking JSON serializability...\n")

    try:
        json.dumps(weather)
        print("✅ Weather data is JSON serializable")
    except TypeError as e:
        print("❌ Weather data is NOT serializable:", e)

    try:
        json.dumps(air)
        print("✅ Air quality data is JSON serializable")
    except TypeError as e:
        print("❌ Air quality data is NOT serializable:", e)

    try:
        json.dumps(gdp)
        print("✅ GDP data is JSON serializable")
    except TypeError as e:
        print("❌ GDP data is NOT serializable:", e)
