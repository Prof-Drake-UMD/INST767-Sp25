import csv
from api_calls import get_food_nutrition

def transform_nutrition():
    app_id = "e7b7f0e3"
    app_key = "8b5d276a9e6acb387913c6f10d716ca3"

    # Step 1: Extract ingredients from USDA foods
    ingredient_set = set()

    try:
        with open("../output/usda_foods.csv", "r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                description = row["description"].strip().lower()
                if description and len(description.split()) <= 5:  # keep it simple
                    ingredient_set.add(description)
    except FileNotFoundError:
        print("Error: usda_foods.csv not found. Run transform_usda() first.")
        return

    # Step 2: Call Nutritionix for each cleaned ingredient
    with open("../output/nutrition_logs.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["food_name", "calories", "protein", "total_fat", "carbohydrates"])

        for food in sorted(ingredient_set):
            print(f"🔍 Querying: {food}")
            result = get_food_nutrition(food, app_id, app_key)
            for item in result.get("foods", []):
                writer.writerow([
                    item["food_name"],
                    item["nf_calories"],
                    item["nf_protein"],
                    item["nf_total_fat"],
                    item["nf_total_carbohydrate"]
                ])

    print(f"Nutrition logs written for {len(ingredient_set)} USDA-based ingredients.")

if __name__ == "__main__":
    transform_nutrition()
