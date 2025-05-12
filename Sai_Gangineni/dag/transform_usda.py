import csv
from api_calls import search_usda_foods

def transform_usda():
    api_key = "ZwiliNrJ5HBTMcnkPl2lbCkJq1fkS1BBKCjM9CQg"
    queries = [
    "chicken breast", "salmon", "ground beef", "eggs", "greek yogurt",
    "tofu", "lentils", "cottage cheese", "spinach", "broccoli",
    "sweet potato", "avocado", "quinoa", "brown rice", "almonds",
    "peanut butter", "banana", "orange", "milk", "oats"
    ]


    with open("../output/usda_foods.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["fdcId", "description", "brandOwner", "foodCategory", "protein", "fat", "carbs", "calories"])

        for query in queries:
            foods = search_usda_foods(query, api_key)
            for food in foods:
                nutrients = {n['nutrientName']: n['value'] for n in food.get("foodNutrients", [])}
                writer.writerow([
                    food.get("fdcId", ""),
                    food.get("description", ""),
                    food.get("brandOwner", ""),
                    food.get("foodCategory", ""),
                    nutrients.get("Protein", ""),
                    nutrients.get("Total lipid (fat)", ""),
                    nutrients.get("Carbohydrate, by difference", ""),
                    nutrients.get("Energy", "")
                ])

if __name__ == "__main__":
    transform_usda()
