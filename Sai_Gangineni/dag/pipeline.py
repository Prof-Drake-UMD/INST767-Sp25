from transform_nutrition import transform_nutrition
from transform_exercises import transform_exercises
from transform_usda import transform_usda

def main():
    print("Starting pipeline...\n")

    print("Transforming Wger...")
    transform_exercises()
    print("Done with Wger.\n")

    print("Transforming USDA FoodData Central...")
    transform_usda()
    print("Done with USDA.\n")

    print("Transforming Nutritionix...")
    transform_nutrition()
    print("Done with Nutritionix.\n")
    
    print("All CSVs written to output folder!")

if __name__ == "__main__":
    main()
