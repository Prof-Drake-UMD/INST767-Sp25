import requests

# ---------- Nutritionix ----------
def get_food_nutrition(food_name, app_id, app_key):
    url = "https://trackapi.nutritionix.com/v2/natural/nutrients"
    headers = {
        "x-app-id": app_id,
        "x-app-key": app_key,
        "x-remote-user-id": "0",
        "Content-Type": "application/json"
    }
    data = {"query": food_name}
    response = requests.post(url, headers=headers, json=data)
    return response.json() if response.status_code == 200 else {}

# ---------- Wger ----------
def get_exercises_wger():
    url = "https://wger.de/api/v2/exerciseinfo/?language=2&limit=1000"
    response = requests.get(url)
    return response.json().get("results", []) if response.status_code == 200 else []

# ---------- USDA FoodData Central ----------
def search_usda_foods(query, api_key, page_size=5):
    url = f"https://api.nal.usda.gov/fdc/v1/foods/search"
    params = {
        "query": query,
        "pageSize": page_size,
        "api_key": api_key
    }
    response = requests.get(url, params=params)
    return response.json().get("foods", []) if response.status_code == 200 else []

# ---------- Test ----------
if __name__ == "__main__":
    print("Testing Nutritionix...")
    print(get_food_nutrition("apple", "e7b7f0e3", "8b5d276a9e6acb387913c6f10d716ca3"))

    print("Testing Wger...")
    print(get_exercises_wger()[0])

    print("Testing USDA...")
    print(search_usda_foods("salmon", "ZwiliNrJ5HBTMcnkPl2lbCkJq1fkS1BBKCjM9CQg"))
