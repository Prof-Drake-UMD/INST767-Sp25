import requests

# Wger API call
def get_exercises():
    url = "https://wger.de/api/v2/exerciseinfo/?language=2"
    response = requests.get(url)
    return response.json()

# Nutritionix API (replacing old get_nutrition_data with search + item lookup)
def search_food_item(food_query, app_id, app_key):
    url = "https://trackapi.nutritionix.com/v2/search/instant"
    headers = {
        "x-app-id": app_id,
        "x-app-key": app_key
    }
    params = {"query": food_query}
    response = requests.get(url, headers=headers, params=params)
    return response.json()

def get_food_nutrients(item_id, app_id, app_key):
    url = f"https://trackapi.nutritionix.com/v2/search/item?nix_item_id={item_id}"
    headers = {
        "x-app-id": app_id,
        "x-app-key": app_key
    }
    response = requests.get(url, headers=headers)
    return response.json()

# OpenWeatherMap API call
def get_weather_data(city_name, api_key):
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city_name,
        "appid": api_key,
        "units": "metric"
    }
    response = requests.get(url, params=params)
    return response.json()

if __name__ == "__main__":
    # Fill in your API keys here for testing
    nutritionix_app_id = "e7b7f0e3"
    nutritionix_app_key = "8b5d276a9e6acb387913c6f10d716ca3"
    openweather_api_key = "cc39e55645488541724622e038d6a2c3"

    # Test Wger API
    exercises_data = get_exercises()
    print("Exercises Data:", exercises_data)

    # Test Nutritionix API
    search_result = search_food_item("apple", nutritionix_app_id, nutritionix_app_key)
    print("Search Result:", search_result)

    if search_result.get('common'):
        item_id = search_result['common'][0].get('tag_id')
    elif search_result.get('branded'):
        item_id = search_result['branded'][0].get('nix_item_id')
    else:
        print("No food item found for query.")
        item_id = None

    if item_id:
        food_data = get_food_nutrients(item_id, nutritionix_app_id, nutritionix_app_key)
        print("Food Data:", food_data)
    else:
        print("No valid item ID to fetch nutrients.")

    # Test Weather API
    weather_data = get_weather_data("College Park", openweather_api_key)
    print("Weather Data:", weather_data)
