import requests

# --- Open-Meteo Weather API ---
def fetch_weather_forecast(lat=38.9072, lon=-77.0369):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "timezone": "America/New_York"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# --- Open-Meteo Air Quality API ---
def fetch_air_quality():
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": 38.9072,       # Washington, DC
        "longitude": -77.0369,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide",
        "timezone": "America/New_York"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# --- USGS Water Services API ---
def fetch_usgs_water(site="01646500"):
    """
    Fetch streamflow data from USGS.
    Example site: 01323500 (Bronx River at NY Botanical Garden)
    ParameterCd 00060 = streamflow (cubic feet per second)
    """
    url = "https://waterservices.usgs.gov/nwis/iv/"
    params = {
        "sites": site,
        "parameterCd": "00060",
        "format": "json"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# --- Example Execution ---
if __name__ == "__main__":
    print("Fetching weather forecast...")
    weather = fetch_weather_forecast()
    print(weather)

    print("\nFetching Washington D.C. air quality data...")
    air_quality = fetch_air_quality()
    print(air_quality[:3])  # print just a few entries

    print("\nFetching USGS water data...")
    usgs_water = fetch_usgs_water()
    print(usgs_water)
