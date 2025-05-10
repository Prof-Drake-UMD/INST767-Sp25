from transform_exercises import transform_exercises
from transform_nutrition import transform_nutrition
from transform_weather import transform_weather

def main():
    print("Starting pipeline...")
    transform_exercises()
    print("Finished exercises transformation.")
    transform_nutrition()
    print("Finished nutrition transformation.")
    transform_weather()
    print("Finished weather transformation.")
    print("Pipeline completed. CSV files generated.")

if __name__ == "__main__":
    main()

