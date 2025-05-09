import csv
from api_calls import search_food_item, get_food_nutrients
import time

def transform_nutrition():
    app_id = "e7b7f0e3"
    app_key = "8b5d276a9e6acb387913c6f10d716ca3"

    # Step 1: search for "apple"
    search_result = search_food_item("apple", app_id, app_key)
    print("Search result:", search_result)

    # Use the first common food item returned (or branded if you prefer)
    if search_result.get('common'):
        item = search_result['common'][0]
        item_id = item.get('tag_id')  # For common foods
    elif search_result.get('branded'):
        item = search_result['branded'][0]
        item_id = item.get('nix_item_id')  # For branded foods
    else:
        print("No food item found for query.")
        return

    if not item_id:
        print("No item ID found in response.")
        return

    # Step 2: fetch nutrition info for that item
    food_data = get_food_nutrients(item_id, app_id, app_key)
    print("Food data:", food_data)

    # Step 3: Write to CSV (adjust field names as per response)
    with open('output/nutrition_logs.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'food_name', 'calories', 'total_fat', 'saturated_fat',
                         'protein', 'carbohydrates', 'sugars', 'serving_weight_grams', 'logged_at'])

        food = food_data.get('foods', [{}])[0]
        writer.writerow([
            food.get('nix_item_id', item_id),
            food.get('food_name', item['food_name']),
            food.get('nf_calories', 0),
            food.get('nf_total_fat', 0),
            food.get('nf_saturated_fat', 0),
            food.get('nf_protein', 0),
            food.get('nf_total_carbohydrate', 0),
            food.get('nf_sugars', 0),
            food.get('serving_weight_grams', 0),
            time.strftime('%Y-%m-%dT%H:%M:%SZ')
        ])
