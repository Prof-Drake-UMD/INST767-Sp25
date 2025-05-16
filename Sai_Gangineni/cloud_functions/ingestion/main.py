import base64
import csv
import json
import os
import requests
from google.cloud import pubsub_v1, bigquery
import google.auth

# ----------------------------- NUTRITIONIX -----------------------------

def get_nutritionix_data(food_name):
    url = "https://trackapi.nutritionix.com/v2/natural/nutrients"
    headers = {
        "x-app-id": os.environ.get("NUTRITIONIX_APP_ID", "MISSING"),
        "x-app-key": os.environ.get("NUTRITIONIX_APP_KEY", "MISSING"),
        "x-remote-user-id": "0",
        "Content-Type": "application/json"
    }

    print("Nutritionix headers being sent:", headers)

    payload = {"query": food_name}
    response = requests.post(url, headers=headers, json=payload)

    print("Nutritionix response:", response.status_code, response.text)

    return response.json() if response.status_code == 200 else {}


def nutritionix_subscriber(event, context):
    if "data" not in event:
        print("No data received.")
        return

    food_name = base64.b64decode(event["data"]).decode("utf-8")
    print(f"Received from nutrition-topic: {food_name}")
    result = get_nutritionix_data(food_name)
    print("📦 Nutritionix data returned:", result)
    foods = result.get("foods", [])

    if not foods:
        print("No nutrition data returned.")
        return

    client = bigquery.Client()
    project_id = google.auth.default()[1]
    table_id = f"{project_id}.inst767_final.nutrition_logs"

    rows_to_insert = []
    for food in foods:
        rows_to_insert.append({
            "food_name": food.get("food_name", ""),
            "calories": food.get("nf_calories", 0),
            "protein": food.get("nf_protein", 0),
            "fat": food.get("nf_total_fat", 0),
            "carbs": food.get("nf_total_carbohydrate", 0)
        })

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("BigQuery insert errors:", errors)
    else:
        print(f"Inserted {len(rows_to_insert)} nutrition row(s) into BigQuery.")

def nutritionix_publisher(request):
    from google.cloud import pubsub_v1, bigquery

    try:
        client = bigquery.Client()
        publisher = pubsub_v1.PublisherClient()

        project_id = google.auth.default()[1]
        topic_path = publisher.topic_path(project_id, "nutrition-topic")

        # Query descriptions from BigQuery USDA table
        query = """
            SELECT DISTINCT description
            FROM `{}.inst767_final.usda_foods`
            WHERE description IS NOT NULL
            LIMIT 25
        """.format(project_id)

        results = client.query(query).result()

        count = 0
        for row in results:
            food = row["description"]
            if food and len(food.split()) <= 5:
                print(f"Publishing to nutrition-topic: {food}")
                publisher.publish(topic_path, data=food.encode("utf-8"))
                count += 1

        return f"Published {count} Nutritionix ingredients from BigQuery USDA data."

    except Exception as e:
        print(f"Nutritionix publisher error: {str(e)}")
        return f"500 ERROR: {str(e)}", 500



# ----------------------------- USDA -----------------------------

def get_usda_data(query, api_key):
    url = "https://api.nal.usda.gov/fdc/v1/foods/search"
    params = {"query": query, "pageSize": 2, "api_key": api_key}
    response = requests.get(url, params=params)
    return response.json() if response.status_code == 200 else {}

def usda_subscriber(event, context):
    if "data" not in event:
        print("No data received.")
        return

    query = base64.b64decode(event["data"]).decode("utf-8")
    print(f"Received from usda-topic: {query}")
    api_key = os.environ["USDA_API_KEY"]
    result = get_usda_data(query, api_key)

    foods = result.get("foods", [])
    if not foods:
        print("No USDA results.")
        return

    client = bigquery.Client()
    project_id = google.auth.default()[1]
    table_id = f"{project_id}.inst767_final.usda_foods"

    rows_to_insert = []
    for food in foods:
        rows_to_insert.append({
            "description": food.get("description", ""),
            "brand": food.get("brandOwner", ""),
            "fdc_id": str(food.get("fdcId", "")),
            "category": food.get("foodCategory", ""),
            "calories": food.get("foodNutrients", [{}])[0].get("value", 0),
            "protein": next((n.get("value", 0) for n in food.get("foodNutrients", []) if n.get("nutrientName") == "Protein"), 0),
            "fat": next((n.get("value", 0) for n in food.get("foodNutrients", []) if n.get("nutrientName") == "Total lipid (fat)"), 0),
            "carbs": next((n.get("value", 0) for n in food.get("foodNutrients", []) if n.get("nutrientName") == "Carbohydrate, by difference"), 0)
        })

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("BigQuery insert errors:", errors)
    else:
        print(f"Inserted {len(rows_to_insert)} USDA row(s) into BigQuery.")

def usda_publisher(request):
    publisher = pubsub_v1.PublisherClient()
    project_id = google.auth.default()[1]
    topic_path = publisher.topic_path(project_id, "usda-topic")

    queries = [
        "chicken breast", "salmon", "ground beef", "eggs", "greek yogurt",
        "tofu", "lentils", "cottage cheese", "spinach", "broccoli",
        "sweet potato", "avocado", "quinoa", "brown rice", "almonds",
        "peanut butter", "banana", "orange", "milk", "oats"
    ]
    for q in queries:
        print(f"Publishing to usda-topic: {q}")
        publisher.publish(topic_path, data=q.encode("utf-8"))

    return f"Published {len(queries)} USDA queries."


# ----------------------------- WGER -----------------------------

def get_wger_exercises(muscle):
    url = f"https://wger.de/api/v2/exerciseinfo/?language=2&limit=1000"
    response = requests.get(url)
    if response.status_code == 200:
        results = response.json().get("results", [])
        return [ex for ex in results if muscle.lower() in ex.get("category", {}).get("name", "").lower()]
    return []

def wger_subscriber(event, context):
    if "data" not in event:
        print("No data received.")
        return

    muscle = base64.b64decode(event["data"]).decode("utf-8")
    print(f"Received from wger-topic: {muscle}")
    exercises = get_wger_exercises(muscle)
    if not exercises:
        print("No Wger results.")
        return

    client = bigquery.Client()
    project_id = google.auth.default()[1]
    table_id = f"{project_id}.inst767_final.exercises"

    rows_to_insert = []
    for ex in exercises:
        rows_to_insert.append({
            "exercise_id": ex.get("id", 0),
            "name": next((t.get("name", "") for t in ex.get("translations", []) if t.get("language") == 2), ""),
            "category": ex.get("category", {}).get("name", ""),
            "equipment": ", ".join([e["name"] for e in ex.get("equipment", [])]),
            "description": next((t.get("description", "") for t in ex.get("translations", []) if t.get("language") == 2), "")
        })

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("BigQuery insert errors:", errors)
    else:
        print(f"Inserted {len(rows_to_insert)} Wger row(s) into BigQuery.")

def wger_publisher(request):
    publisher = pubsub_v1.PublisherClient()
    project_id = google.auth.default()[1]
    topic_path = publisher.topic_path(project_id, "wger-topic")

    muscles = ["arms", "legs", "shoulders", "back", "chest", "abs", "glutes"]
    for m in muscles:
        print(f"Publishing to wger-topic: {m}")
        publisher.publish(topic_path, data=m.encode("utf-8"))

    return f"Published {len(muscles)} Wger muscle groups."
